package com.codecommit
package dm

import java.io._
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

trait BucketStoreComponent extends BucketComponent {
  private type Pair = (Long, Array[Byte])
  
  def bucketStore: BucketStore
  
  trait BucketStore {
    def read(file: String)(back: Array[Pair] => Unit)
    def write(file: String, pairs: Array[Pair])
  }
}

// TODO lru queue for paging buckets
trait MutableBucketControlComponent extends BucketStoreComponent {
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
      bucketStore.read(file) { pairs =>
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

trait NonRotatingBucketStoreComponent extends BucketStoreComponent {
  private type Pair = (Long, Array[Byte])
  
  lazy val bucketStore = new BucketStore with AsyncWorker {
    lazy val Priority = 9
    lazy val Name = "bucket-persister"
    
    private val queue = new LinkedBlockingQueue[WorkUnit]
    
    override def read(file: String)(back: Array[Pair] => Unit) {
      queue.offer(Read(file, back))
    }
    
    private def performRead(file: String, back: Array[Pair] => Unit) {
      var is: InputStream = null
      try {
        is = new BufferedInputStream(new FileInputStream(file))
        
        val result = new Array[Pair](1 << bucketControl.Power)
        for (i <- 0 until result.length) {
          val longBytes = new Array[Byte](8)
          is.read(longBytes)
          val key = bytesToLong(longBytes)
          
          if (key != -1L) {       // note: this means that -1 is an invalid key!
            val intBytes = new Array[Byte](4)
            is.read(intBytes)
            
            val value = new Array[Byte](bytesToInt(intBytes))
            is.read(value)
            
            result(i) = (key, value)
          }
        }
        
        back(result)
      } catch {
        case e: IOException =>    // something very bad happened, log and move on
      } finally {
        if (is != null) {
          is.close()
        }
      }
    }
    
    private def bytesToLong(bytes: Array[Byte]) = {
      ((((((((0L | bytes(0))
        << 8 | bytes(1))
          << 8 | bytes(2))
            << 8 | bytes(3))
              << 8 | bytes(4))
                << 8 | bytes(5))
                  << 8 | bytes(6))
                    << 8 | bytes(7))
    }
    
    private def bytesToInt(bytes: Array[Byte]) = {
      ((((0 | bytes(0))
        << 8 | bytes(1))
          << 8 | bytes(2))
            << 8 | bytes(3))
    }
    
    private def longToBytes(lng: Long): Array[Byte] = {
      val back = new Array[Byte](8)
      val mask = 0xFFL
      back(7) = (lng & mask).toByte
      back(6) = ((lng & (mask << 8)) >>> 8).toByte
      back(5) = ((lng & (mask << 16)) >>> 16).toByte
      back(4) = ((lng & (mask << 24)) >>> 24).toByte
      back(3) = ((lng & (mask << 32)) >>> 32).toByte
      back(2) = ((lng & (mask << 40)) >>> 40).toByte
      back(1) = ((lng & (mask << 48)) >>> 48).toByte
      back(0) = ((lng & (mask << 56)) >>> 56).toByte
      back
    }
    
    private def intToBytes(i: Int): Array[Byte] = {
      val back = new Array[Byte](4)
      val mask = 0xFF
      back(3) = (i & mask).toByte
      back(2) = ((i & (mask << 8)) >>> 8).toByte
      back(1) = ((i & (mask << 16)) >>> 16).toByte
      back(0) = ((i & (mask << 24)) >>> 32).toByte
      back
    }
    
    override def write(file: String, pairs: Array[Pair]) {
      queue.offer(Write(file, pairs))
    }
    
    private def performWrite(file: String, pairs: Array[Pair]) {
      var os: OutputStream = null
      try {
        os = new BufferedOutputStream(new FileOutputStream(file))
        for (pair <- pairs) {
          if (pair == null) {
            os.write(longToBytes(-1L))
          } else {
            val (key, value) = pair
            os.write(longToBytes(key))
            os.write(intToBytes(value.length))
            os.write(value)
          }
        }
      } catch {
        case e: IOException =>    // something very bad happened, log and move on
      } finally {
        if (os != null) {
          os.close()
        }
      }
    }
    
    protected def runOnce() {
      queue.poll(5, TimeUnit.SECONDS) match {
        case null => ()
        case Read(file, back) => performRead(file, back)
        case Write(file, pairs) => performWrite(file, pairs)
      }
    }
    
    protected def awaitWork() {}
  }
  
  private sealed trait WorkUnit
  private case class Read(file: String, back: Array[Pair] => Unit) extends WorkUnit
  private case class Write(file: String, pairs: Array[Pair]) extends WorkUnit
}
