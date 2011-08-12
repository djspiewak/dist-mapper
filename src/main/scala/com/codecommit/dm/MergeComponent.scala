package com.codecommit
package dm

import java.util.concurrent.Semaphore

import scala.collection.generic.CanBuildFrom

trait MergeComponent extends JournalComponent {
  def merger: Merger
  
  trait Merger {
    def spawn()
    def kill()
  }
}

trait SeqMergeComponent extends MergeComponent {
  type JournalRep <: Seq[Op]
  
  def repCanBuildFrom: CanBuildFrom[JournalRep, Op, JournalRep]
  
  /**
   * Assumes that `ops` is non-empty!
   */
  protected def mergeAll[That](ops: JournalRep)(implicit cbf: CanBuildFrom[JournalRep, Op, That]): That = {
    val builder = cbf()
    builder += ops.reduceLeft { _ + _ }     // could potentially parallelize this
    builder.result()
  }
}

trait ThreadedMergeComponent extends SeqMergeComponent with MutableJournalComponent {
  lazy val MergerThreadPriority = 3
  
  override lazy val merger = new Merger with Runnable {
    private val thread = {
      val back = new Thread(this, "journal-merger")
      back.setPriority(MergerThreadPriority)
      back
    }
    
    private var killSignal = new Semaphore(1)       // a rather odd use of a semaphore...
    
    def spawn() {
      thread.start()
    }
    
    def kill() {
      killSignal.acquire()
    }
    
    def run() {
      val shouldDie = killSignal.tryAcquire()
      if (!shouldDie) {     // sanity check right at the beginning
        try {
          while (!killSignal.hasQueuedThreads) {
            val ops = journal.journal
            
            if (!ops.isEmpty) {
              val ops2 = mergeAll(ops)(repCanBuildFrom)
              journal.compareAndSwapPartial(ops2, ops.last)       // assumes mutable component
            } else {
              wait(100)      // TODO do something clever, possibly with semaphores
            }
          }
        } finally {
          killSignal.release()
        }  
      }
    }
  }
}
