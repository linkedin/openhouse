package com.linkedin.openhouse.spark.lineage

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.util.Try

/**
 * Reflection helpers used to read fields off Spark/Iceberg logical plan nodes.
 *
 * The OpenHouse Spark runtime is compiled once against Spark 3.1 and shipped on both Spark 3.1 and
 * Spark 3.5 (`openhouse-spark-3.5-runtime` repackages the 3.1 jar). Between those releases several
 * command nodes changed their constructor signature - `CreateTableAsSelect`, for example, went from
 * `(catalog, tableName, partitioning, query, ...)` to `(name, partitioning, query, tableSpec, ...)`,
 * and `MergeIntoTable` gained a `notMatchedBySourceActions` field. Scala pattern matching on those
 * case classes compiles down to a call to a fixed-arity `unapply`, so a destructuring match would
 * link against 3.1 and blow up at runtime on 3.5.
 *
 * Field *accessor* methods, on the other hand, kept both their name and their return type across
 * those versions. Reading through accessors therefore keeps a single compiled artifact working on
 * every supported Spark line, and additionally lets us read Iceberg's own rewritten plan nodes
 * (`MergeIntoIcebergTable`, `UpdateIcebergTable`, ...) which are not on the compile classpath at
 * all but expose the same accessor names.
 */
private[lineage] object PlanAccessors {

  def invoke[T](target: AnyRef, method: String)(implicit tag: reflect.ClassTag[T]): Option[T] =
    Try {
      val m = target.getClass.getMethod(method)
      m.setAccessible(true)
      m.invoke(target)
    }.toOption.flatMap {
      case null => None
      case v if tag.runtimeClass.isInstance(v) => Some(v.asInstanceOf[T])
      case _ => None
    }

  def hasMethod(target: AnyRef, method: String): Boolean =
    Try(target.getClass.getMethod(method)).isSuccess

  def plan(target: AnyRef, method: String): Option[LogicalPlan] = invoke[LogicalPlan](target, method)

  def expression(target: AnyRef, method: String): Option[Expression] =
    invoke[Expression](target, method)

  def boolean(target: AnyRef, method: String): Option[Boolean] =
    invoke[java.lang.Boolean](target, method).map(_.booleanValue())

  /** Reads a `Seq[T]` accessor and converts it to a Scala [[Seq]] the JVM can hand back to us. */
  def seq[T](target: AnyRef, method: String): Seq[T] =
    Try {
      val m = target.getClass.getMethod(method)
      m.setAccessible(true)
      m.invoke(target).asInstanceOf[scala.collection.Seq[T]]
    }.toOption.map(_.toList).getOrElse(Nil)

  /** Reads an `Option[T]` accessor. */
  def option[T](target: AnyRef, method: String): Option[T] =
    Try {
      val m = target.getClass.getMethod(method)
      m.setAccessible(true)
      m.invoke(target).asInstanceOf[Option[T]]
    }.toOption.flatten
}
