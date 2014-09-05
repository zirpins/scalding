package com.twitter.scalding.macroimpl

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._
import scala.util.{ Try => BasicTry }

import cascading.tuple.{ Tuple => CTuple, TupleEntry }

import com.twitter.scalding._

object Macro {
  def caseClassTupleSetter[T]: TupleSetter[T] = macro caseClassTupleSetterImpl[T]
  def caseClassTupleSetterImpl[T](c: Context)(implicit tag: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] = {
    import c.universe._
    if (!isCaseClass[T](c)) {
      throw new IllegalArgumentException("Type paramter of caseClassTupleSetter must be a case class")
    }
    val params = tag.tpe.declaration(nme.CONSTRUCTOR).asMethod.paramss.head
    val set =
      params
        .zipWithIndex
        .map {
          case (sym, idx) =>
            val name = newTermName(sym.name.toString)
            sym.typeSignature match {
              case tpe if tpe =:= typeOf[String] => q"""tup.setString(${idx}, t.$name)"""
              case tpe if tpe =:= typeOf[Boolean] => q"""tup.setBoolean(${idx}, t.$name)"""
              case tpe if tpe =:= typeOf[Short] => q"""tup.setShort(${idx}, t.$name)"""
              case tpe if tpe =:= typeOf[Int] => q"""tup.setInteger(${idx}, t.$name)"""
              case tpe if tpe =:= typeOf[Long] => q"""tup.setLong(${idx}, t.$name)"""
              case tpe if tpe =:= typeOf[Float] => q"""tup.setFloat(${idx}, t.$name)"""
              case tpe if tpe =:= typeOf[Double] => q"""tup.setDouble(${idx}, t.$name)"""
              case tpe if tpe.typeSymbol.asInstanceOf[ClassSymbol].isCaseClass => q"""tup.set(${idx}, caseClassTupleSetter[${tpe}](t.$name))"""
              case _ => q"""tup.set(${idx}, t.$name)"""
            }
        }
        .foldLeft(q"") { (cum, next) => q"""$cum;$next""" }

    val res = q"""
    new TupleSetter[${tag.tpe}] {
      override def apply(t: ${tag.tpe}): CTuple = {
        val tup = CTuple.size(${params.size})
        $set
        tup
      }
      override def arity = ${params.size}
    }
    """
    c.Expr[TupleSetter[T]](res)
  }

  def caseClassTupleConverter[T]: TupleConverter[T] = macro caseClassTupleConverterImpl[T]
  def caseClassTupleConverterImpl[T](c: Context)(implicit tag: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] = {
    import c.universe._
    if (!isCaseClass[T](c)) {
      throw new IllegalArgumentException("Type paramter of caseClassTupleConverter must be a case class")
    }
    val params = tag.tpe.declaration(nme.CONSTRUCTOR).asMethod.paramss.head
    val gets =
      params
        .zipWithIndex
        .map {
          case (sym, idx) =>
            val name = newTermName(sym.name.toString)
            sym.typeSignature match {
              case tpe if tpe =:= typeOf[String] => q"""tup.getString(${idx})"""
              case tpe if tpe =:= typeOf[Boolean] => q"""tup.getBoolean(${idx})"""
              case tpe if tpe =:= typeOf[Short] => q"""tup.getShort(${idx})"""
              case tpe if tpe =:= typeOf[Int] => q"""tup.getInteger(${idx})"""
              case tpe if tpe =:= typeOf[Long] => q"""tup.getLong(${idx})"""
              case tpe if tpe =:= typeOf[Float] => q"""tup.getFloat(${idx})"""
              case tpe if tpe =:= typeOf[Double] => q"""tup.getDouble(${idx})"""
              case tpe if tpe.typeSymbol.asInstanceOf[ClassSymbol].isCaseClass => q"""caseClassTupleConverter[${tpe}](new TupleEntry(tup.getObject(${idx}).asInstanceOf[CTuple]))"""
              case tpe => q"""tup.getObject(${idx}).asInstanceOf[${tpe}]"""
            }
        }

    val res = q"""
    new TupleConverter[${tag.tpe}] {
      override def apply(t: TupleEntry): ${tag.tpe} = {
        val tup = t.getTuple()
        ${tag.tpe.typeSymbol.companionSymbol}(..$gets)
      }
      override def arity = ${params.size}
    }
    """
    c.Expr[TupleConverter[T]](res)
  }

  def isCaseClass[T](c: Context)(implicit tag: c.WeakTypeTag[T]): Boolean =
    BasicTry { tag.tpe.typeSymbol.asInstanceOf[ClassSymbol].isCaseClass }.toOption.getOrElse(false)
}
