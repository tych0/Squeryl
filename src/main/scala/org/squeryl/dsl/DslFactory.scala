package org.squeryl.dsl

import ast._
import org.squeryl.internals.{OutMapper}
import java.sql.ResultSet
import java.util.Date


trait DslFactory
  extends TypeArithmetic
    with SqlFunctions {

  protected def createLeafNodeOfScalarIntType(i: IntType): NumericalExpression[IntType]
  protected def createLeafNodeOfScalarIntOptionType(i: Option[IntType]): NumericalExpression[Option[IntType]]

  protected def createLeafNodeOfScalarDoubleType(d: DoubleType): NumericalExpression[DoubleType]
  protected def createLeafNodeOfScalarDoubleOptionType(d: Option[DoubleType]): NumericalExpression[Option[DoubleType]]

  protected def createLeafNodeOfScalarFloatType(d: FloatType): NumericalExpression[FloatType]
  protected def createLeafNodeOfScalarFloatOptionType(d: Option[FloatType]): NumericalExpression[Option[FloatType]]

  protected def createLeafNodeOfScalarStringType(s: StringType): StringExpression[StringType]
  protected def createLeafNodeOfScalarStringOptionType(s: Option[StringType]): StringExpression[Option[StringType]]

  protected def createLeafNodeOfScalarLongType(s: LongType): NumericalExpression[LongType]
  protected def createLeafNodeOfScalarLongOptionType(s: Option[LongType]): NumericalExpression[Option[LongType]]

  protected def createLeafNodeOfScalarBooleanType(s: BooleanType): BooleanExpression[BooleanType]
  protected def createLeafNodeOfScalarBooleanOptionType(s: Option[BooleanType]): BooleanExpression[Option[BooleanType]]

  protected def createLeafNodeOfScalarDateType(d: DateType): DateExpression[DateType]
  protected def createLeafNodeOfScalarDateOptionType(d: Option[DateType]): DateExpression[Option[DateType]]

  // expose Factory Methods implicit :
  // ScalarNode Types :
  implicit def int2ScalarInt(i: IntType) = createLeafNodeOfScalarIntType(i)

  implicit def double2ScalarDouble(d: DoubleType) = createLeafNodeOfScalarDoubleType(d)

  implicit def float2ScalarFloat(d: FloatType) = createLeafNodeOfScalarFloatType(d)

  implicit def string2ScalarString(s: StringType) = createLeafNodeOfScalarStringType(s)

  implicit def long2ScalarLong(l: LongType) = createLeafNodeOfScalarLongType(l)

  implicit def bool2ScalarBoolean(b: BooleanType) = createLeafNodeOfScalarBooleanType(b)

  implicit def date2ScalarDate(b: DateType) = createLeafNodeOfScalarDateType(b)

  implicit def optionInt2ScalarInt(i: Option[IntType]) = createLeafNodeOfScalarIntOptionType(i)

  implicit def optionLong2ScalarLong(i: Option[LongType]) = createLeafNodeOfScalarLongOptionType(i)

  implicit def optionString2ScalarString(i: Option[StringType]) = createLeafNodeOfScalarStringOptionType(i)

  implicit def optionDouble2ScalarDouble(i: Option[DoubleType]) = createLeafNodeOfScalarDoubleOptionType(i)

  implicit def optionFloat2ScalarFloat(i: Option[FloatType]) = createLeafNodeOfScalarFloatOptionType(i)

  implicit def optionBoolean2ScalarBoolean(i: Option[BooleanType]) = createLeafNodeOfScalarBooleanOptionType(i)

  implicit def optionDate2ScalarDate(i: Option[DateType]) = createLeafNodeOfScalarDateOptionType(i)

  
  // List Conversion implicits don't vary with the choice of
  // column/field types, so they don't need to be overridable factory methods :

  //TODO: replace lists with NonNumerical and Numerical for type inference that is more SQL like  
  implicit def listOfInt2ListInt(l: List[IntType]) =
    new ConstantExpressionNodeList[IntType](l) with ListInt

  implicit def listOfDouble2ListDouble(l: List[DoubleType]) =
    new ConstantExpressionNodeList[DoubleType](l) with ListDouble

  implicit def listOfFloat2ListFloat(l: List[FloatType]) =
    new ConstantExpressionNodeList[FloatType](l) with ListFloat

  implicit def listOfLong2ListLong(l: List[LongType]) =
    new ConstantExpressionNodeList[LongType](l) with ListLong

  implicit def listOfString2ListString(l: List[StringType]) =
    new ConstantExpressionNodeList[StringType](l) with ListString

  implicit def listOfDate2ListDate(l: List[DateType]) =
    new ConstantExpressionNodeList[DateType](l) with ListDate

  implicit def typedExpression2OrderByArg[E <% TypedExpressionNode[_]](e: E) = new OrderByArg(e)

  implicit def orderByArg2OrderByExpression(a: OrderByArg) = new OrderByExpression(a)

}