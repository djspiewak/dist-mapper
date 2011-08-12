package com.codecommit
package dm

import java.io.{OutputStream, FileOutputStream, BufferedOutputStream, IOException}

trait JournalPersistenceComponent extends JournalComponent {
  def journalPersister: JournalPersister
  
  trait JournalPersister {
    def spawn()
    def kill()
  }
}

/**
 * Runs journal persistence on its own thread.  Basically assumes that the journal
 * is stored on its own device (so that seeks do not conflict with the main IO
 * thread).  Note that we're not doing anything clever to expire old journal files,
 * we're just glomming new journals into the directory.  We could be smarter, but
 * we aren't right now.
 */
trait ThreadedJournalPersistenceComponent extends JournalPersistenceComponent {
  def JournalPersistenceDelay: Long
  
  def makeJournalFilename: String
  
  override lazy val journalPersister = new JournalPersister with AsyncWorker {
    val Priority = 3
    val Name = "journal-persister"
    
    protected def runOnce() {
      var os: OutputStream = null      // gross!
      try {
        os = new BufferedOutputStream(new FileOutputStream(makeJournalFilename))
        journalSerializer.write(journal.journal, os)
      } catch {
        case e: IOException => // TODO do something cool
      } finally {
        if (os != null) {
          os.close()
        }
      }
    }
    
    protected def awaitWork() {
      Thread.sleep(JournalPersistenceDelay)
    }
  }
}
