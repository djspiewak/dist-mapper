package com.codecommit
package dm

trait KeyValueComponent {
  def store: KeyValue
  
  trait KeyValue {
    def get(key: Long)(back: Option[Array[Byte]] => Unit)
    def contains(key: Long)(back: Boolean => Unit)
    def put(key: Long, value: Array[Byte])
    def delete(key: Long)
  }
}

trait JournaledKeyValueComponent extends KeyValueComponent with JournalComponent {
  type JournalRep <: Seq[Op]
  
  override def store: JournaledKeyValue
  
  trait JournaledKeyValue extends KeyValue {
    final override def get(key: Long)(back: Option[Array[Byte]] => Unit) {
      val result = journal.journal.view map { _ get key } collectFirst { case Some(res) => res }
      result match {
        case Some(res) => back(res)
        case None => getFromStore(key)(back)
      }
    }
    
    protected def getFromStore(key: Long)(back: Option[Array[Byte]] => Unit)
    
    final override def contains(key: Long)(back: Boolean => Unit) {
      val result = journal.journal.view map { _ contains key } collectFirst { case Some(res) => res }
      result match {
        case Some(res) => back(res)
        case None => containsInStore(key)(back)
      }
    }
    
    protected def containsInStore(key: Long)(back: Boolean => Unit)
    
    final override def put(key: Long, value: Array[Byte]) {
      journal.push(Modify(key, value))
    }
    
    final override def delete(key: Long) {
      journal.push(Delete(key))
    }
  }
}
