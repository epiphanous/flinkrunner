//package io.epiphanous.flinkrunner.model
//import enumeratum.{Enum, EnumEntry}
//import reflect.runtime.universe._
//
//import scala.util.matching.Regex
//
//case class JPContextObj[T](value: T, name: String) {
//  val isList = value.isInstanceOf[Seq]
//}
//case class JPContext[
//    R <: Product with Ordered[R],
//    C <: Ordered[C],
//    V <: Ordered[V]
//  ](root: JPContextObj[R],
//    current: JPContextObj[C],
//    isRoot: Boolean,
//    value: V) {
//  val context = if (isRoot) root else current
//  val isList  = context.isList
//}
//
//sealed trait JPNode {
//  def evaluate[C: TypeTag](c: C): Seqval[V]
//}
//
///**
//  * $.source[3][?( @.type == "garmin" )]
//  * JPCondition(
//  * $ JPFilterExpr[C=R, V=Boolean].evaluate(Seq(root)) => Seq(true)
//  *
//  * @tparam R
//  * @tparam CIN
//  */
//abstract case class JPPredicate[C <: Product with Ordered[C]]()
//    extends JPNode[C, Boolean]
//
//case class JPComparisonPredicate[C <: Product with Ordered[C], E <: Ordered[E]](
//    op: ComparisonOperator,
//    left: JPExpression[C, E],
//    right: JPExpression[C, E])
//    extends JPPredicate[C] {
//  import ComparisonOperator._
//
//  def evaluate(c: Seq[C]) = {
//    val lhs    = left.evaluate(c)
//    val rhs    = right.evaluate(c)
//    val result = lhs.zip(rhs).map { case (l, r) => l.compare(r) }
//    Seq(op match {
//      case Equal              => result.forall(_ == 0)
//      case NotEqual           => !result.contains(0)
//      case LessThan           => result.forall(_ < 0)
//      case LessThanOrEqual    => result.forall(_ <= 0)
//      case GreaterThan        => result.forall(_ > 0)
//      case GreaterThanOrEqual => result.forall(_ >= 0)
//    })
//  }
//}
//
//case class JPMatchPredicate(
//    expr: JPExpression[String],
//    pattern: Regex,
//    wantsMatch: Boolean = true)
//    extends JPPredicate {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) = {
//    val value = expr.evaluate(c)
//    pattern.findFirstIn(value).isDefined == wantsMatch
//  }
//}
//
//case class JPInPredicate[T](value: JPExpression[T], list: List[JPExpression[T]])
//    extends JPPredicate {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) =
//    list.map(_.evaluate(c)).contains(value.evaluate(c))
//}
//
//case class JPTruthyPredicate[T](expr: JPExpression[T]) extends JPPredicate {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) =
//    Option(expr.evaluate(c)).nonEmpty
//}
//
//case class JPParenPredicate(condition: JPCondition) extends JPPredicate {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) =
//    condition.evaluate(c)
//}
//
//case class JPConditionNot(predicate: JPPredicate) extends JPNode[Boolean] {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) =
//    !predicate.evaluate(c)
//}
//
//case class JPConditionAnd(conditionNots: List[JPConditionNot])
//    extends JPNode[Boolean] {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) =
//    conditionNots.forall(_.evaluate(c))
//}
//
//case class JPCondition(conditionAnds: List[JPConditionAnd])
//    extends JPNode[Boolean] {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) =
//    conditionAnds.exists(_.evaluate(c))
//}
//
//case class JPArrayIndex(indexes: List[Int]) extends JPNode[List[_ <: Product]] {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) = c match {
//    case Right(list) => indexes.map(ix => list(ix))
//    case Left(a) =>
//      throw new RuntimeException(
//        s"attempt to take elements (${indexes.mkString(",")}) from list in scalar context")
//  }
//}
//
//case class JPFromToIndex(from: Int, to: Int)
//    extends JPNode[List[_ <: Product]] {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) = c match {
//    case Right(list) => list.slice(from, to)
//    case Left(a) =>
//      throw new RuntimeException(
//        s"attempt to take slice list element $from to $to in scalar context")
//  }
//}
//
//case class JPFromStartIndex(from: Int) extends JPNode[List[_ <: Product]] {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) = c match {
//    case Right(list) => list.slice(from, list.length)
//    case Left(a) =>
//      throw new RuntimeException(
//        s"attempt to take slice from element $from to end of list in scalar context")
//  }
//}
//
//case class JPFirstElementsIndex(n: Int) extends JPNode[List[_ <: Product]] {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) = c match {
//    case Right(list) => list.take(n)
//    case Left(a) =>
//      throw new RuntimeException(
//        s"attempt to take first $n elements from list in scalar context")
//  }
//}
//
//case class JPLastElementsIndex(n: Int) extends JPNode[List[_ <: Product]] {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) = c match {
//    case Right(list) => list.takeRight(n)
//    case Left(a) =>
//      throw new RuntimeException(
//        s"attempt to take last $n elements from list in scalar context")
//  }
//}
//
//case class JPFilterExpression(condition: JPCondition) extends JPNode[Boolean] {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) =
//    condition.evaluate(c)
//}
//
//case class JPAtLengthExpression(n: Int) extends JPNode[_ <: Product] {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) = c match {
//    case Right(list) => list(math.max(list.size - n, 0))
//    case Left(_) =>
//      throw new RuntimeException(s"@.length - $n invalid in scalar context")
//  }
//}
//
//case class JPProperty(name: String, isRecursive: Boolean = false)
//    extends JPNode[(String, Boolean)] {
//  override def evaluate[C <: Product](c: Either[C, List[C]]) =
//    (name, isRecursive)
//}
//
//abstract case class JPExpression[C <: Product with Ordered[C], E <: Ordered[E]](
//  ) extends JPNode[C, E]
//
//case class JPLiteral[C <: Product with Ordered[C], E <: Ordered[E]](literal: E)
//    extends JPExpression[C, E] {
//  def evaluate(c: Seq[C]) = Seq(literal)
//}
//
//case class JPLens[L <: Product, T](getter: L => T) extends JPExpression[T] {
//  def evaluate[C <: L](c: Either[C, List[C]]) = getter(c)
//}
//
//sealed trait ComparisonOperator extends EnumEntry
//object ComparisonOperator extends Enum[ComparisonOperator] {
//  override def values = findValues
//  case object Equal              extends ComparisonOperator
//  case object NotEqual           extends ComparisonOperator
//  case object LessThan           extends ComparisonOperator
//  case object LessThanOrEqual    extends ComparisonOperator
//  case object GreaterThan        extends ComparisonOperator
//  case object GreaterThanOrEqual extends ComparisonOperator
//}
