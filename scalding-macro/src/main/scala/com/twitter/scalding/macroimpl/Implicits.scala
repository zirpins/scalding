package com.twitter.scalding.macroimpl

import scala.language.experimental.macros

import com.twitter.scalding._

/**
 * This is an object which can be imported to make the macro functionality defined in this package
 * available implicitly.
 */
object Implicits {
  implicit def materializeCaseClassTupleSetter[T]: TupleSetter[T] = macro Macro.caseClassTupleSetterImpl[T]
  implicit def materializeCaseClassTupleConverter[T]: TupleConverter[T] = macro Macro.caseClassTupleConverterImpl[T]
}
