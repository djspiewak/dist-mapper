package com.codecommit
package dm

sealed trait Op {
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
  protected def collapse(op: Op) = op match {
    case Modify(`key`, _) => Some(op)
    case Delete(`key`) => Some(op)
    case _ => None
  }
}

case class Delete(key: Long) extends Op {
  protected def collapse(op: Op) = op match {
    case Modify(`key`, _) => Some(op)
    case Delete(`key`) => Some(this)
    case _ => None
  }
}

case class Composite(ops: Vector[Op]) extends Op {
  override def +(op: Op) = op match {
    case Composite(_) => (ops :\ op) { _ + _ }      // very inefficient, can be done better
    case _ => super.+(op)
  }
  
  protected def collapse(op: Op) = None
}
