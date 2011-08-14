package com.codecommit
package dm

import java.io._
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
  
trait BucketStore {
  private type Pair = (Long, Array[Byte])
  
  def read(file: String, power: Int)(back: Array[Pair] => Unit)
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
      var is: InputStream = null
      try {
        is = new BufferedInputStream(new FileInputStream(file))
        
        val result = new Array[Pair](1 << power)
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
        case Read(file, power, back) => performRead(file, power, back)
        case Write(file, pairs) => performWrite(file, pairs)
      }
    }
    
    protected def awaitWork() {}
  }
  
  private sealed trait WorkUnit
  private case class Read(file: String, power: Int, back: Array[Pair] => Unit) extends WorkUnit
  private case class Write(file: String, pairs: Array[Pair]) extends WorkUnit
}
