package com.twitter.scalding.macros.impl

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._
import scala.util.{ Try => BasicTry }

import com.twitter.scalding._
import com.twitter.scalding.macros.IsCaseClass

/**
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */
object MacroImpl {
  def isCaseClassImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[IsCaseClass[T]] = {
    import c.universe._
    if (isCaseClassType(c)(T.tpe)) {
      c.Expr[IsCaseClass[T]](q"""new _root_.com.twitter.scalding.macros.impl.MacroGeneratedIsCaseClass[$T] { }""")
    } else {
      c.abort(c.enclosingPosition, "Type parameter is not a case class")
    }
  }

  def caseClassTupleSetterNoProof[T]: TupleSetter[T] = macro caseClassTupleSetterNoProofImpl[T]

  def caseClassTupleSetterImpl[T](c: Context)(proof: c.Expr[IsCaseClass[T]])(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] =
    caseClassTupleSetterNoProofImpl(c)(T)

  def caseClassTupleSetterNoProofImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] = {
    import c.universe._
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
              case tpe if isCaseClassType(c)(tpe) => q"""
                tup.set(
                  ${idx},
                  _root_.com.twitter.scalding.macros.impl.MacroImpl.caseClassTupleSetterNoProof[$tpe](t.$m)
                )
                """
              case _ => q"""tup.set(${idx}, t.$m)"""
            }
        }

    c.Expr[TupleSetter[T]](q"""
    new _root_.com.twitter.scalding.macros.impl.MacroGeneratedTupleSetter[$T] {
      override def apply(t: $T): _root_.cascading.tuple.Tuple = {
        val tup = _root_.cascading.tuple.Tuple.size(${set.size})
        ..$set
        tup
      }
      override def arity = ${set.size}
    }
    """)
  }

  def caseClassTupleConverterNoProof[T]: TupleConverter[T] = macro caseClassTupleConverterNoProofImpl[T]

  def caseClassTupleConverterImpl[T](c: Context)(proof: c.Expr[IsCaseClass[T]])(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] =
    caseClassTupleConverterNoProofImpl(c)(T)

  def caseClassTupleConverterNoProofImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] = {
    import c.universe._
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
                _root_.com.twitter.scalding.macros.impl.MacroImpl.caseClassTupleConverterNoProof[$tpe](
                  new _root_.cascading.tuple.TupleEntry(tup.getObject(${idx}).asInstanceOf[_root_.cascading.tuple.Tuple])
                )
                """
              case tpe => q"""tup.getObject(${idx}).asInstanceOf[$tpe]"""
            }
        }

    c.Expr[TupleConverter[T]](q"""
    new _root_.com.twitter.scalding.macros.impl.MacroGeneratedTupleConverter[$T] {
      override def apply(t: _root_.cascading.tuple.TupleEntry): $T = {
        val tup = t.getTuple()
        ${T.tpe.typeSymbol.companionSymbol}(..$get)
      }
      override def arity = ${get.size}
    }
    """)
  }

  def isCaseClassType(c: Context)(tpe: c.universe.Type): Boolean =
    BasicTry { tpe.typeSymbol.asClass.isCaseClass }.toOption.getOrElse(false)
}

/**
 * These traits allow us to inspect if a given TupleSetter of TupleConverter was generated. This is particularly
 * useful for testing.
 */
trait MacroGenerated
trait MacroGeneratedTupleSetter[T] extends TupleSetter[T] with MacroGenerated
trait MacroGeneratedTupleConverter[T] extends TupleConverter[T] with MacroGenerated
trait MacroGeneratedIsCaseClass[T] extends IsCaseClass[T] with MacroGenerated
