package com.codecommit
package dm

sealed trait Op {
  def get(key: Long): Option[Option[Array[Byte]]]
  
  def contains(key: Long) = get(key) map { _.isDefined }

  def +(op: Op) = op match {
    case Composite(ops) => {
      val reduction = ops map collapse zip ops
      val isReduced = reduction exists { case (opt, _) => opt.isDefined }
      val ops2 = reduction map { case (opt, x) => opt getOrElse x }
      
      if (isReduced) Composite(ops2) else Composite(op +: ops2)
    }
    
    case _ => collapse(op) getOrElse Composite(Vector(this, op))
  }
  
  protected def collapse(op: Op): Option[Op]
}

case class Modify(key: Long, value: Array[Byte]) extends Op {
  def get(key: Long) = if (this.key == key) Some(Some(value)) else None
  
  protected def collapse(op: Op) = op match {
    case Modify(`key`, _) => Some(op)
    case Delete(`key`) => Some(op)
    case _ => None
  }
}

case class Delete(key: Long) extends Op {
  def get(key: Long) = if (this.key == key) Some(None) else None
  
  protected def collapse(op: Op) = op match {
    case Modify(`key`, _) => Some(op)
    case Delete(`key`) => Some(this)
    case _ => None
  }
}

// note that the order of the operations doesn't actually matter, since they are guaranteed collapsed
case class Composite(ops: Vector[Op]) extends Op {
  def get(key: Long) = ops.view map { _ get key } collectFirst { case Some(res) => res }
  
  override def +(op: Op) = op match {
    case Composite(_) => (ops :\ op) { _ + _ }      // very inefficient, can be done better
    case _ => super.+(op)
  }
  
  protected def collapse(op: Op) = None
}
