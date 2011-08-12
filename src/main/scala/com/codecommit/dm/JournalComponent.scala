package com.codecommit
package dm

/**
 * Cake abstraction for the journal.  Intended to be implemented using a mutable
 * backing store, but ''could'' be implemented immutably.
 */
trait JournalComponent {
  type JournalRep
  
  def journal: Journal

  trait Journal {
    def journal: JournalRep
    
    def push(op: Op): Journal
    def peek: Option[Op]
    def pop: Journal
    
    def compareAndSwapPartial(journal: JournalRep, marker: Op): Boolean
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
    private var _journal: JournalRep = Vector()
    
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
}
