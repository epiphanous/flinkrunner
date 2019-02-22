//package io.epiphanous.flinkrunner.model
//
//import com.mdsol.sensorflink.model.jp.ComparisonOperator._
//import com.typesafe.scalalogging.LazyLogging
//import io.epiphanous.antlr4.jp._
//import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
//import org.antlr.v4.runtime.tree.TerminalNode
//
//import scala.collection.immutable.ListMap
//import scala.collection.mutable
//import scala.reflect.runtime.universe._
//import scala.util.Try
//import collection.JavaConverters._
//
//class JsonPath[P <: Product, R: TypeTag](source: String)
//    extends JSONPathListener
//    with LazyLogging {
//
//  val errors = mutable.ArrayBuffer.empty[String]
//
//  val fields = fieldsOf[P]
//
//  def fieldsOf[T <: Product](implicit tag: TypeTag[T]) = _fieldsOf(tag.tpe)
//
//  def _fieldsOf(tpe: Type) = {
//    val constructorSymbol = tpe.decl(termNames.CONSTRUCTOR)
//    val defaultConstructor =
//      if (constructorSymbol.isMethod) constructorSymbol.asMethod
//      else {
//        val ctors = constructorSymbol.asTerm.alternatives
//        ctors.map(_.asMethod).find(_.isPrimaryConstructor).get
//      }
//
//    ListMap[String, Type]() ++ defaultConstructor.paramLists
//      .reduceLeft(_ ++ _)
//      .map { sym =>
//        sym.name.toString -> tpe.member(sym.name).asMethod.returnType
//      }
//  }
//
//  val jpNodeStack: mutable.ArrayStack[JPNode[_]] = mutable.ArrayStack()
//
//  lazy val parser = {
//    val input       = CharStreams.fromString(source)
//    val lexer       = new JSONPathLexer(input)
//    val tokenStream = new CommonTokenStream(lexer)
//    new JSONPathParser(tokenStream)
//  }
//
//  @volatile
//  var parsed = false
//
//  @volatile
//  var root: JPLens[P, R] = _
//
//  def parse = Try {
//    parser.jsonPath()
//    if (errors.nonEmpty) {
//      throw new Throwable(s"invalid json path expression: <$source>\n${errors
//        .mkString("  - ", "\n  - ", "\n")}")
//    }
//    root = jpNodeStack.pop().asInstanceOf[JPLens[P, R]]
//    parsed = true
//  }
//
//  def evaluate(p: P): Try[R] = {
//    Try {
//      if (!parsed) parse
//      root.evaluate(Left(p))
//    }
//  }
//
//  def evaluate(listp: List[P]): Try[R] = {
//    Try {
//      if (!parsed) parse
//      root.evaluate(Right(listp))
//    }
//  }
//
//  override def exitJsonPath(ctx: JSONPathParser.JsonPathContext): Unit = {}
//
//  override def exitPropertyPath(
//      ctx: JSONPathParser.PropertyPathContext
//    ): Unit = {}
//
//  override def exitDotProperty(ctx: JSONPathParser.DotPropertyContext): Unit =
//    jpNodeStack.push(JPProperty(textOf(ctx.Identifier())))
//
//  override def exitBracketedProperty(
//      ctx: JSONPathParser.BracketedPropertyContext
//    ): Unit =
//    jpNodeStack.push(JPProperty(textOf(ctx.Identifier())))
//
//  override def exitRecursiveProperty(
//      ctx: JSONPathParser.RecursivePropertyContext
//    ): Unit =
//    jpNodeStack.push(JPProperty(textOf(ctx.Identifier()), isRecursive = true))
//
//  override def exitRecursiveAllProperties(
//      ctx: JSONPathParser.RecursiveAllPropertiesContext
//    ): Unit =
//    jpNodeStack.push(JPProperty("*", isRecursive = true))
//
//  override def exitDotAllProperties(
//      ctx: JSONPathParser.DotAllPropertiesContext
//    ): Unit =
//    jpNodeStack.push(JPProperty("*"))
//
//  override def exitBracketedAllElements(
//      ctx: JSONPathParser.BracketedAllElementsContext
//    ): Unit =
//    jpNodeStack.push(JPProperty("*"))
//
//  override def exitArrayIndex(ctx: JSONPathParser.ArrayIndexContext): Unit =
//    jpNodeStack.push(
//      JPArrayIndex(ctx.IntegerLiteral().asScala.map(t => intOf(t)).toList))
//
//  override def exitFromToRange(ctx: JSONPathParser.FromToRangeContext): Unit =
//    jpNodeStack.push(
//      JPFromToIndex(intOf(ctx.IntegerLiteral(0)), intOf(ctx.IntegerLiteral(1))))
//
//  override def exitFromStartRange(
//      ctx: JSONPathParser.FromStartRangeContext
//    ): Unit =
//    jpNodeStack.push(JPFromStartIndex(intOf(ctx.IntegerLiteral())))
//
//  override def exitFirstElements(
//      ctx: JSONPathParser.FirstElementsContext
//    ): Unit =
//    jpNodeStack.push(JPFirstElementsIndex(intOf(ctx.IntegerLiteral())))
//
//  override def exitLastElements(ctx: JSONPathParser.LastElementsContext): Unit =
//    jpNodeStack.push(JPLastElementsIndex(intOf(ctx.IntegerLiteral())))
//
//  override def exitFilterExpression(
//      ctx: JSONPathParser.FilterExpressionContext
//    ): Unit = {
//    val condition = jpNodeStack.pop().asInstanceOf[JPCondition]
//    jpNodeStack.push(JPFilterExpression(condition))
//  }
//
//  override def exitCondition(ctx: JSONPathParser.ConditionContext): Unit = {
//    val list = ctx
//      .conditionAnd()
//      .asScala
//      .map(_ => jpNodeStack.pop().asInstanceOf[JPConditionAnd])
//      .toList
//    jpNodeStack.push(JPCondition(list))
//  }
//
//  override def exitConditionAnd(
//      ctx: JSONPathParser.ConditionAndContext
//    ): Unit = {
//    val list = ctx
//      .conditionNot()
//      .asScala
//      .map(_ => jpNodeStack.pop().asInstanceOf[JPConditionNot])
//      .toList
//    jpNodeStack.push(JPConditionAnd(list))
//  }
//
//  override def exitConditionNot(
//      ctx: JSONPathParser.ConditionNotContext
//    ): Unit = {
//    val predicate = jpNodeStack.pop().asInstanceOf[JPPredicate]
//    jpNodeStack.push(JPConditionNot(predicate))
//  }
//
//  override def exitComparisonPredicate(
//      ctx: JSONPathParser.ComparisonPredicateContext
//    ): Unit = {
//    val rhs = jpNodeStack.pop()
//    val lhs = jpNodeStack.pop()
//    val opc = ctx.comparisonOp()
//    val op = List(
//      Option(opc.Equal()).map(_ => Equal),
//      Option(opc.NotEqual()).map(_ => NotEqual),
//      Option(opc.LessThan()).map(_ => LessThan),
//      Option(opc.LessThanOrEqual()).map(_ => LessThanOrEqual),
//      Option(opc.GreaterThan()).map(_ => GreaterThan),
//      Option(opc.GreaterThanOrEqual()).map(_ => GreaterThanOrEqual)
//    ).flatten.head
//    jpNodeStack.push(JPComparisonPredicate(op, lhs, rhs))
//  }
//
//  override def exitMatchPredicate(
//      ctx: JSONPathParser.MatchPredicateContext
//    ): Unit = {
//    val expr       = jpNodeStack.pop().asInstanceOf[JPExpression[String]]
//    val wantsMatch = Option(ctx.matchOp().Match()).nonEmpty
//    val pattern    = textOf(ctx.RegularExpressionLiteral()).r
//    jpNodeStack.push(JPMatchPredicate(expr, pattern, wantsMatch))
//  }
//
//  override def exitInPredicate(ctx: JSONPathParser.InPredicateContext): Unit = {
//    val nodes = Range(0, ctx.expression().asScala.size - 2)
//      .map(_ => jpNodeStack.pop().asInstanceOf[JPExpression[_]])
//      .toList
//    val value = jpNodeStack.pop().asInstanceOf[JPExpression[_]]
//    jpNodeStack.push(JPInPredicate(value, nodes.reverse))
//  }
//
//  override def exitTruthyPredicate(
//      ctx: JSONPathParser.TruthyPredicateContext
//    ): Unit = {
//    val expr = jpNodeStack.pop().asInstanceOf[JPExpression[_]]
//    jpNodeStack.push(JPTruthyPredicate(expr))
//  }
//
//  override def exitParenPredicate(
//      ctx: JSONPathParser.ParenPredicateContext
//    ): Unit = {
//    val condition = jpNodeStack.pop().asInstanceOf[JPCondition]
//    jpNodeStack.push(JPParenPredicate(condition))
//  }
//
//  override def exitIntegerLiteral(
//      ctx: JSONPathParser.IntegerLiteralContext
//    ): Unit = {
//    val num = intOf(ctx.IntegerLiteral())
//    jpNodeStack.push(JPLiteral[Int](num))
//  }
//
//  override def exitDecimalLiteral(
//      ctx: JSONPathParser.DecimalLiteralContext
//    ): Unit = {
//    val num = textOf(ctx.DecimalLiteral()).toDouble
//    jpNodeStack.push(JPLiteral[Double](num))
//  }
//
//  override def exitHexLiteral(ctx: JSONPathParser.HexLiteralContext): Unit = {
//    val s = Integer.parseInt(textOf(ctx.HexIntegerLiteral()), 16)
//    jpNodeStack.push(JPLiteral[Int](s))
//  }
//
//  override def exitOctalLiteral(
//      ctx: JSONPathParser.OctalLiteralContext
//    ): Unit = {
//    val s = Integer.parseInt(textOf(ctx.OctalIntegerLiteral()), 8)
//    jpNodeStack.push(JPLiteral[Int](s))
//  }
//
//  override def exitOctal2Literal(
//      ctx: JSONPathParser.Octal2LiteralContext
//    ): Unit = {
//    val s = Integer.parseInt(textOf(ctx.OctalIntegerLiteral2()), 8)
//    jpNodeStack.push(JPLiteral[Int](s))
//  }
//
//  override def exitBinaryLiteral(
//      ctx: JSONPathParser.BinaryLiteralContext
//    ): Unit = {
//    val s = Integer.parseInt(textOf(ctx.BinaryIntegerLiteral()), 2)
//    jpNodeStack.push(JPLiteral[Int](s))
//  }
//
//  override def exitStringLiteral(
//      ctx: JSONPathParser.StringLiteralContext
//    ): Unit = {
//    val s = textOf(ctx.StringLiteral())
//    jpNodeStack.push(JPLiteral[String](s))
//  }
//
//  override def exitBooleanLiteral(
//      ctx: JSONPathParser.BooleanLiteralContext
//    ): Unit = {
//    val b = textOf(ctx.BooleanLiteral()) == "true"
//    jpNodeStack.push(JPLiteral[Boolean](b))
//  }
//
//  override def exitNullLiteral(ctx: JSONPathParser.NullLiteralContext): Unit =
//    jpNodeStack.push(JPLiteral[Null](null))
//
//  def textOf(node: TerminalNode, stripQuotes: Boolean = true) = {
//    val text = node.getText.trim
//    (if (stripQuotes) text.replaceAll("""(^[\"']|['\"]$""", "") else text).trim
//  }
//
//  def intOf(node: TerminalNode) =
//    textOf(node, false).toInt
//}
