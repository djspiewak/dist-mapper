package com.codecommit
package dm

import java.io.{InputStream, OutputStream}

/**
 * Cake abstraction for the journal.  Intended to be implemented using a mutable
 * backing store, but ''could'' be implemented immutably.
 */
trait JournalComponent {
  type JournalRep
  
  def journal: Journal
  def journalSerializer: JournalSerializer
  
  def readInitialJournalState: JournalRep

  trait Journal {
    def journal: JournalRep
    
    def push(op: Op): Journal
    def peek: Option[Op]
    def pop: Journal
    
    def compareAndSwapPartial(journal: JournalRep, marker: Op): Boolean
  }
  
  trait JournalSerializer {
    def read(is: InputStream): JournalRep
    def write(rep: JournalRep, os: OutputStream)
  }
}

/**
 * Mutable implementation of [[com.codecommit.dm.JournalComponent]].  Handles
 * synchronization and sane concurrent semantics.
 */
trait MutableJournalComponent extends JournalComponent {
  type JournalRep = Vector[Op]
  
  override lazy val journal = new Journal {
    private val lock = new AnyRef
    private var _journal: JournalRep = readInitialJournalState
    
    def journal = _journal
    
    def push(op: Op) = lock synchronized {
      _journal = _journal :+ op
      this
    }
    
    def peek = journal.headOption
    
    def pop = lock synchronized {
      _journal = _journal.tail
      this
    }
    
    def compareAndSwapPartial(journal2: JournalRep, marker: Op) = lock synchronized {
      val (left, right) = journal span { op => !(marker eq op) }       // use object identity for efficiency
      
      // if the marker op is gone, then overwriting would be destructive
      if (!right.isEmpty) {
        _journal = journal2 ++ right
        true
      } else {
        false
      }
    }
  }
  
  override lazy val journalSerializer = new JournalSerializer {
    def read(is: InputStream) = Vector()               // TODO stub
    def write(rep: JournalRep, os: OutputStream) {}    // TODO stub
  }
}
