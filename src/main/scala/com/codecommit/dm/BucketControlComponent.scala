package com.codecommit
package dm

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

// TODO lru queue for paging buckets
trait MutableBucketControlComponent extends BucketComponent with BucketStoreComponent {
  private type Pair = (Long, Array[Byte])
  
  private var _trie: Trie = InMemoryTip(0, "-", new Array[Pair](1 << bucketControl.Power))
  private val lock = new AnyRef
  
  def trie = _trie
  
  // necessary since we may be writing the try in a callback from the IO thread
  private def trie_=(trie: Trie) = lock synchronized {
    _trie = trie
  }
  
  lazy val bucketControl = new BucketControl with AsyncWorker {
    lazy val Priority = 4
    lazy val Name = "bucket-control"
    
    private lazy val Mask = (1 << Power) - 1
    
    private val queue = new LinkedBlockingQueue[WorkUnit]
    
    def swapWithNew(trie: Trie, buckets: Tip*) {
      queue.offer(SwapWithNew(trie, buckets))
    }
    
    private def performSwapWithNew(trie2: Trie, buckets: Seq[Tip]) {
      trie = trie2
      
      for (InMemoryTip(_, file, pairs) <- buckets) {
        bucketStore.write(file, pairs)
      }
    }
    
    def touchTip(bucket: Tip) {
      queue.offer(TouchTip(bucket))
    }
    
    private def performTouchTip(bucket: Tip) {
      // would touch bucket in LRU
    }
    
    def unpage(key: Long, file: String)(back: InMemoryTip => Unit) {
      queue.offer(Unpage(key, file, back))
    }
    
    private def performUnpage(key: Long, file: String, back: InMemoryTip => Unit) {
      bucketStore.read(file, Power) { pairs =>
        val (trie2, tip) = placeBucket(trie, key, file, pairs)
        trie = trie2
        back(tip)
      }
    }
    
    private def placeBucket(trie: Trie, shiftedKey: Long, file: String, pairs: Array[Pair]): (Trie, InMemoryTip) = trie match {
      case Branch(level, children) => {
        val (trie, tip) = placeBucket(children((shiftedKey & Mask).toInt), shiftedKey >>> Power, file, pairs)
        
        val children2 = new Array[Trie](children.length)
        System.arraycopy(children, 0, children2, 0, children.length)
        children2((shiftedKey & Mask).toInt) = trie
        (Branch(level, children), tip)
      }
      
      case tip @ InMemoryTip(_, _, _) => {
        // since we don't collapse on delete, we must have already paged this one
        (tip, tip)
      }
      
      case PagedTip(level, _) => {
        val back = InMemoryTip(level, file, pairs)
        (back, back)
      }
    }
    
    protected def runOnce() {
      queue.poll(5, TimeUnit.SECONDS) match {
        case null => ()
        case SwapWithNew(trie, buckets) => performSwapWithNew(trie, buckets)
        case TouchTip(bucket) => performTouchTip(bucket)
        case Unpage(key, file, back) => performUnpage(key, file, back)
      }
    }
    
    protected def awaitWork() {}
    
    private sealed trait WorkUnit
    private case class SwapWithNew(trie: Trie, buckets: Seq[Tip]) extends WorkUnit
    private case class TouchTip(bucket: Tip) extends WorkUnit
    private case class Unpage(key: Long, file: String, back: InMemoryTip => Unit) extends WorkUnit
  }
}
