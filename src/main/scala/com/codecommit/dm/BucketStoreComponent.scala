package com.codecommit
package dm

import java.io._
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
  
trait BucketStore {
  private type Pair = (Long, Array[Byte])
  
  /**
   * Reads the specified file and CPS-returns an array of key/value pairs of
   * length `1 << power`.  Will not CPS-return for reads that raise an
   * [[java.util.IOException]].  Note that this method defines a default
   * implementation which is unsuitable for asynchronous implementations!
   */
  def read(file: String, power: Int)(back: Array[Pair] => Unit) {
    val result = new Array[Pair](1 << power)
    var i = 0
    foreach(file :: Nil, power) { pair =>
      result(i) = pair
      i += 1
    }
    back(result)
  }
  
  /**
   * Iterates over all the key/value pairs in a sequence of files, invoking the
   * given function for each in order.  The default implementation of this
   * function should be suitable for most implementations (including asynchronous).
   * 
   * It is possible to implement the map/reduce paradigm using this function.
   */
  def foreach(files: Seq[String], power: Int)(f: Pair => Unit) {
    multiForeach(files :: Nil, power) { _ foreach f }
  }
  
  /**
   * Iterates over all of the key/value pairs in each set of files simultaneously,
   * invoking the specified function once for each key/value pair.  If a key/value
   * pair from a file in the same super-sequence step as another file has a key
   * equal to a key in the other file (or files), those key/value pairs will be
   * passed to the function in a single sequence.
   *
   * It is possible to implement both the union and intersect operations in terms
   * of this function as it represents a generic, possibly-asynchronous simultaneous
   * traversal of multiple on-disk maps.
   */
  def multiForeach(files: Seq[Seq[String]], power: Int)(f: List[Pair] => Unit)
  
  /**
   * Writes the given array of key/value pairs to the specified file.  In the
   * case where an [[java.util.IOException]] is raised by the corresponding IO,
   * the exception will be silently swallowed.  This is due to the
   * potentially-asynchronous nature of the implementation.
   */
  def write(file: String, pairs: Array[Pair])
}

/**
 * Cake abstraction for the low-level bucket IO mechanism.  This is separate
 * from bucket control and the actual data structure to allow sharing across
 * multiple maps.  It is intended that all maps will have a separate instance
 * of the [[com.codecommit.dm.BucketComponent]] with [[com.codecommit.dm.JournalComponent]]
 * cake, but they will ''share'' a single `BucketStoreComponent` to avoid running
 * IO on more than one thread simultaneously.
 */
trait BucketStoreComponent {
  def bucketStore: BucketStore
}

trait NonRotatingBucketStoreComponent extends BucketStoreComponent {
  private type Pair = (Long, Array[Byte])
  
  override lazy val bucketStore = new BucketStore with AsyncWorker {
    lazy val Priority = 9
    lazy val Name = "bucket-persister"
    
    private val queue = new LinkedBlockingQueue[WorkUnit]
    
    override def read(file: String, power: Int)(back: Array[Pair] => Unit) {
      queue.offer(Read(file, power, back))
    }
    
    private def performRead(file: String, power: Int, back: Array[Pair] => Unit) {
      back(readFile(file, power))
    }
    
    private def readFile(file: String, power: Int): Array[Pair] = {
      val result = new Array[Pair](1 << power)
      
      var is: InputStream = null
      try {
        is = new BufferedInputStream(new FileInputStream(file))
        
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
      } catch {
        case e: IOException =>    // something very bad happened, log and move on
      } finally {
        if (is != null) {
          is.close()
        }
      }
      
      result
    }
    
    override def multiForeach(files: Seq[Seq[String]], power: Int)(f: List[Pair] => Unit) {
      queue.offer(MultiForeach(files map { fs => (Nil, fs.view) }, power, f))      // get the view for fast head/tail
    }
    
    private def performMultiForeach(files: Seq[(List[Pair], Seq[String])], power: Int, f: List[Pair] => Unit) {
      val files2 = files map {
        case (Nil, fileNames) if !fileNames.isEmpty =>
          (readFile(fileNames.head, power).toList, fileNames.tail)
        
        case other => other
      }
      
      // pages through the buckets in parallel, sorts and groups the results
      def loop(files: Seq[(List[Pair], Seq[String])]) {
        // first, check to see if we have exhausted a bucket with more still to read
        if (files forall { case (Nil, xs) => xs.isEmpty case _ => true }) {
          // pull out the head pairs from each bucket
          val (pairs, files2) = files map {
            case (hd :: tail, files) => (Some(hd), (tail, files))
            case (Nil, files) => (None, (Nil, files))         // note: these file lists are empty!
          } unzip
          
          val sorted = pairs.flatten sortWith { case ((key1, _), (key2, _)) => key1 < key2 }
          
          // merge equal-keyed pairs into singular lists
          val collapsed = sorted.foldRight(List[List[Pair]]()) {
            case ((key1, value1), ((key2, value2) :: tail) :: acc) if key1 == key2 =>
              ((key1, value1) :: (key2, value2) :: tail) :: acc
            
            case ((key1, value1), acc) => List((key1, value1)) :: acc
          }
          
          collapsed foreach f
          
          loop(files2)
        } else {
          // one (or more) buckets are empty
          queue.offer(MultiForeach(files, power, f))          // interleave to avoid long-term IO starvation
        }
      }
      
      loop(files2)
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
        case Read(file, power, back) => performRead(file, power, back)
        case MultiForeach(files, power, f) => performMultiForeach(files, power, f)
        case Write(file, pairs) => performWrite(file, pairs)
      }
    }
    
    protected def awaitWork() {}
    
    // utility functions
    
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
  }
  
  private sealed trait WorkUnit
  private case class Read(file: String, power: Int, back: Array[Pair] => Unit) extends WorkUnit
  private case class MultiForeach(files: Seq[(List[Pair], Seq[String])], power: Int, f: List[Pair] => Unit) extends WorkUnit
  private case class Write(file: String, pairs: Array[Pair]) extends WorkUnit
}
