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
  override lazy val merger = new Merger with AsyncWorker {
    val Priority = 3
    val Name = "journal-merger"
    
    protected def runOnce() {
      val ops = journal.journal
      
      if (!ops.isEmpty) {
        val ops2 = mergeAll(ops)(repCanBuildFrom)
        journal.compareAndSwapPartial(ops2, ops.last)       // assumes mutable component
      }
    }
    
    protected def awaitWork() {
      Thread.sleep(100)         // TODO do something clever (preferably *much* more clever)
    }
  }
}
