package com.codecommit
package dm

import java.util.concurrent.Semaphore

trait AsyncWorker {
  val Priority: Int
  val Name: String
  
  private val killSignal = new Semaphore(1)
  
  private lazy val thread = {
    val back = new Thread(new Runnable {
      def run() {
        runRepeatedly()
      }
    }, Name)
    back.setPriority(Priority)
    back
  }
  
  def spawn() {
    thread.start()
  }
  
  def kill() {
    killSignal.acquire()
  }
  
  protected def runOnce()
  protected def awaitWork()
  
  private def runRepeatedly() {
    if (!killSignal.tryAcquire()) {     // sanity check right at the beginning
      try {
        while (!killSignal.hasQueuedThreads) {
          awaitWork()
          runOnce()
        }
      } finally {
        killSignal.release()
      }  
    }
  }
}
