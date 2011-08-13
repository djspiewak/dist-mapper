package com.codecommit
package dm

trait BucketComponent {
  private type Pair = (Long, Array[Byte])
  
  private lazy val Mask = (1 << bucketControl.Power) - 1
  
  def bucketControl: BucketControl
  
  trait BucketControl {
    private val Base64 = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+="
    
    lazy val Power = 6        // 64 pairs per bucket
    
    def fileForKey(level: Int, key: Long): String = {
      if (level == 0) {
        "-"
      } else {
        val chars = for (i <- 1 to level; val shifted = key >>> i)
          yield Base64.charAt((shifted & Mask).toInt)
        chars.mkString
      }
    }
    
    def swapWithNew(trie: Trie, buckets: Tip*)
    def touchTip(bucket: Tip)
    def unpage(key: Long, file: String)(back: InMemoryTip => Unit)
  }
  
  sealed trait Trie {
    def get(key: Long, shiftedKey: Long)(back: Option[Array[Byte]] => Unit)
    
    def contains(key: Long, shiftedKey: Long)(back: Boolean => Unit)
    
    /**
     * Assumed to be called in a serial fashion (e.g. in a single thread with a
     * work queue).
     */
    def put(key: Long, value: Array[Byte], shiftedKey: Long, rebuild: Trie => Trie = identity)
    
    /**
     * Assumed to be called in a serial fashion (e.g. in a single thread with a
     * work queue).
     */
    def delete(key: Long, shiftedKey: Long, rebuild: Trie => Trie = identity)
  }
  
  sealed trait Tip extends Trie
  
  case class Branch(level: Int, children: Array[Trie]) extends Trie {
    def get(key: Long, shiftedKey: Long)(back: Option[Array[Byte]] => Unit) {
      val child = selectChild(shiftedKey)
      if (child == null)
        back(None)
      else
        child.get(key, shiftedKey >>> bucketControl.Power)(back)
    }
    
    def contains(key: Long, shiftedKey: Long)(back: Boolean => Unit) {
      val child = selectChild(shiftedKey)
      if (child == null)
        back(false)
      else
        child.contains(key, shiftedKey >>> bucketControl.Power)(back)
    }
    
    def put(key: Long, value: Array[Byte], shiftedKey: Long, rebuild: Trie => Trie) {
      def rebuild2(child2: Trie) = {
        val children2 = new Array[Trie](children.length)
        System.arraycopy(children, 0, children2, 0, children.length)
        children2((shiftedKey & Mask).toInt) = child2
        rebuild(Branch(level, children2))
      }
      
      val child = selectChild(shiftedKey)
      if (child == null) {
        val level2 = level + 1
        val arr = new Array[Pair](1 << bucketControl.Power)
        arr(((shiftedKey >>> bucketControl.Power) & Mask).toInt) = (key, value)
        
        val bucket = InMemoryTip(level2, bucketControl.fileForKey(level2, key), arr)
        bucketControl.swapWithNew(rebuild2(bucket), bucket)
      } else {
        child.put(key, value, shiftedKey >>> bucketControl.Power, rebuild2)
      }
    }
    
    def delete(key: Long, shiftedKey: Long, rebuild: Trie => Trie) {
      def rebuild2(child2: Trie) = {
        val children2 = new Array[Trie](children.length)
        System.arraycopy(children, 0, children2, 0, children.length)
        children2((shiftedKey & Mask).toInt) = child2
        rebuild(Branch(level, children2))
      }
      
      val child = selectChild(shiftedKey)
      if (child != null) {
        child.delete(key, shiftedKey >>> bucketControl.Power, rebuild2)
      }
    }
    
    private def selectChild(shiftedKey: Long) = children((shiftedKey & Mask).toInt)
  }
  
  case class InMemoryTip(level: Int, file: String, pairs: Array[Pair]) extends Tip {
    def get(key: Long, shiftedKey: Long)(back: Option[Array[Byte]] => Unit) {
      bucketControl.touchTip(this)
      
      val pair = selectPair(shiftedKey)
      if (pair == null) {
        back(None)
      } else {
        val (posKey, posValue) = pair
        if (posKey == key)
          back(Some(posValue))
        else
          back(None)
      }
    }
    
    def contains(key: Long, shiftedKey: Long)(back: Boolean => Unit) {
      get(key, shiftedKey) { opt => back(opt.isDefined) }
    }
    
    def put(key: Long, value: Array[Byte], shiftedKey: Long, rebuild: Trie => Trie) {
      lazy val newBucket = {
        val pairs2 = new Array[Pair](pairs.length)
        System.arraycopy(pairs, 0, pairs2, 0, pairs.length)
        pairs2((shiftedKey & Mask).toInt) = (key, value)
        
        InMemoryTip(level, file, pairs2)
      }
      
      val pair = selectPair(shiftedKey)
      if (pair == null) {
        bucketControl.swapWithNew(rebuild(newBucket), newBucket)
      } else {
        val (posKey, posValue) = pair
        
        if (posKey == key) {
          bucketControl.swapWithNew(rebuild(newBucket), newBucket)
        } else {
          // collision, need to split
          val children = for (pair <- pairs) yield {
            if (pair == null) {
              null
            } else {
              val (key, _) = pair
              val subShift = key >>> (bucketControl.Power * level + 1)
              
              val pairs2 = new Array[Pair](pairs.length)
              pairs2((subShift & Mask).toInt) = pair
              
              val level2 = level + 1
              InMemoryTip(level2, bucketControl.fileForKey(level2, key), pairs2): Tip
            }
          }
          
          val branch = Branch(level, children map { t => t: Trie })
          bucketControl.swapWithNew(rebuild(branch), children filter (null !=): _*)
          
          branch.put(key, value, shiftedKey, rebuild)
        }
      }
    }
    
    // note: currently doesn't make any attempt to recombine dead tips
    def delete(key: Long, shiftedKey: Long, rebuild: Trie => Trie) {
      val pair = selectPair(shiftedKey)
      if (pair != null) {
        val (posKey, posValue) = pair
        if (posKey == key) {
          val pairs2 = new Array[Pair](pairs.length)
          System.arraycopy(pairs, 0, pairs2, 0, pairs.length)
          pairs2((shiftedKey & Mask).toInt) = null
          
          val bucket = InMemoryTip(level, file, pairs2)
          bucketControl.swapWithNew(rebuild(bucket), bucket)
        }
      }
    }
    
    private def selectPair(shiftedKey: Long) = pairs((shiftedKey & Mask).toInt)
  }
  
  case class PagedTip(level: Int, file: String) extends Tip {
    def get(key: Long, shiftedKey: Long)(back: Option[Array[Byte]] => Unit) {
      bucketControl.unpage(key, file) { _.get(key, shiftedKey)(back) }
    }
    
    def contains(key: Long, shiftedKey: Long)(back: Boolean => Unit) {
      bucketControl.unpage(key, file) { _.contains(key, shiftedKey)(back) }
    }
    
    def put(key: Long, value: Array[Byte], shiftedKey: Long, rebuild: Trie => Trie) {
      bucketControl.unpage(key, file) { _.put(key, value, shiftedKey, rebuild) }
    }
    
    def delete(key: Long, shiftedKey: Long, rebuild: Trie => Trie) {
      bucketControl.unpage(key, file) { _.delete(key, shiftedKey, rebuild) }
    }
  }
}
