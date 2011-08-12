package com.codecommit
package dm

trait KeyValueComponent {
  def store: KeyValue
  
  trait KeyValue {
    def get(key: Long): Option[Array[Byte]]
    def contains(key: Long): Boolean
    def put(key: Long, value: Array[Byte])
    def delete(key: Long)
  }
}

trait JournaledKeyValueComponent extends KeyValueComponent with JournalComponent {
  type JournalRep <: Seq[Op]
  
  override def store: JournaledKeyValue
  
  trait JournaledKeyValue extends KeyValue {
    final override def get(key: Long) = {
      val result = journal.journal.view map { _ get key } collectFirst { case Some(res) => res }
      result getOrElse getFromStore(key)
    }
    
    protected def getFromStore(key: Long): Option[Array[Byte]]
    
    final override def contains(key: Long) = {
      val result = journal.journal.view map { _ contains key } collectFirst { case Some(res) => res }
      result getOrElse containsInStore(key)
    }
    
    protected def containsInStore(key: Long): Boolean
    
    final override def put(key: Long, value: Array[Byte]) {
      journal.push(Modify(key, value))
    }
    
    final override def delete(key: Long) {
      journal.push(Delete(key))
    }
  }
}
