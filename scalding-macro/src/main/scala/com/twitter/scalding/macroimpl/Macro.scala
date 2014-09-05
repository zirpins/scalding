package com.twitter.scalding.macroimpl

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._
import scala.util.{ Try => BasicTry }

import com.twitter.scalding._

object Macro {
  def caseClassTupleSetter[T]: TupleSetter[T] = macro caseClassTupleSetterImpl[T]
  def caseClassTupleSetterImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] = {
    import c.universe._
    if (!isCaseClass[T](c)) {
      throw new IllegalArgumentException("Type parameter of caseClassTupleSetter must be a case class")
    }
    val set =
      T.tpe.declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .zipWithIndex
        .map {
          case (m, idx) =>
            m.returnType match {
              case tpe if tpe =:= typeOf[String] => q"""tup.setString(${idx}, t.$m)"""
              case tpe if tpe =:= typeOf[Boolean] => q"""tup.setBoolean(${idx}, t.$m)"""
              case tpe if tpe =:= typeOf[Short] => q"""tup.setShort(${idx}, t.$m)"""
              case tpe if tpe =:= typeOf[Int] => q"""tup.setInteger(${idx}, t.$m)"""
              case tpe if tpe =:= typeOf[Long] => q"""tup.setLong(${idx}, t.$m)"""
              case tpe if tpe =:= typeOf[Float] => q"""tup.setFloat(${idx}, t.$m)"""
              case tpe if tpe =:= typeOf[Double] => q"""tup.setDouble(${idx}, t.$m)"""
              case tpe if isCaseClassType(c)(tpe) => q"""tup.set(${idx}, caseClassTupleSetter[${tpe}](t.$m))"""
              case _ => q"""tup.set(${idx}, t.$m)"""
            }
        }

    val res = q"""
    new _root_.com.twitter.scalding.TupleSetter[$T] {
      override def apply(t: $T): _root_.cascading.tuple.Tuple = {
        val tup = _root_.cascading.tuple.Tuple.size(${set.size})
        ..$set
        tup
      }
      override def arity = ${set.size}
    }
    """
    c.Expr[TupleSetter[T]](res)
  }

  def caseClassTupleConverter[T]: TupleConverter[T] = macro caseClassTupleConverterImpl[T]
  def caseClassTupleConverterImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] = {
    import c.universe._
    if (!isCaseClass[T](c)) {
      throw new IllegalArgumentException("Type parameter of caseClassTupleConverter must be a case class")
    }
    //TODO go over it recursively to get the caseClassTupleConverters that we need. May make more sense
    // to return it when we go over it in this case?
    val get =
      T.tpe.declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m.returnType }
        .zipWithIndex
        .map {
          case (returnType, idx) =>
            returnType match {
              case tpe if tpe =:= typeOf[String] => q"""tup.getString(${idx})"""
              case tpe if tpe =:= typeOf[Boolean] => q"""tup.getBoolean(${idx})"""
              case tpe if tpe =:= typeOf[Short] => q"""tup.getShort(${idx})"""
              case tpe if tpe =:= typeOf[Int] => q"""tup.getInteger(${idx})"""
              case tpe if tpe =:= typeOf[Long] => q"""tup.getLong(${idx})"""
              case tpe if tpe =:= typeOf[Float] => q"""tup.getFloat(${idx})"""
              case tpe if tpe =:= typeOf[Double] => q"""tup.getDouble(${idx})"""
              case tpe if isCaseClassType(c)(tpe) =>
                q"""
                caseClassTupleConverter[${tpe}](
                  new _root_.cascading.tuple.TupleEntry(tup.getObject(${idx})
                    .asInstanceOf[_root_.cascading.tuple.Tuple])
                )
                """
              case tpe => q"""tup.getObject(${idx}).asInstanceOf[${tpe}]"""
            }
        }

    val res = q"""
    new _root_.com.twitter.scalding.TupleConverter[$T] {
      override def apply(t: _root_.cascading.tuple.TupleEntry): $T = {
        val tup = t.getTuple()
        ${T.tpe.typeSymbol.companionSymbol}(..$get)
      }
      override def arity = ${get.size}
    }
    """
    c.Expr[TupleConverter[T]](res)
  }

  def isCaseClass[T](c: Context)(implicit T: c.WeakTypeTag[T]): Boolean = isCaseClassType(c)(T.tpe)

  def isCaseClassType(c: Context)(tpe: c.universe.Type): Boolean =
    BasicTry { tpe.typeSymbol.asClass.isCaseClass }.toOption.getOrElse(false)
}
