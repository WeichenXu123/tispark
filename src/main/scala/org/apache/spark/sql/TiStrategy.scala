/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql


import com.pingcap.tikv.expression.{TiByItem, TiColumnRef, TiExpr}
import com.pingcap.tikv.meta.{TiIndexInfo, TiSelectRequest}
import com.pingcap.tikv.predicates.ScanBuilder
import com.pingcap.tispark.TiUtils._
import com.pingcap.tispark.{BasicExpression, TiDBRelation, TiUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, _}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Cast, Divide, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.{JavaConversions, mutable}


// TODO: Too many hacks here since we hijacks the planning
// but we don't have full control over planning stage
// We cannot pass context around during planning so
// a re-extract needed for pushdown since
// a plan tree might have Join which causes a single tree
// have multiple plan to pushdown
class TiStrategy(context: SQLContext) extends Strategy with Logging {
  val sqlConf = context.conf

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val relations = plan.collect({ case p => p })//选取 所有 符合LogicalRelation的 plan
      .filter(_.isInstanceOf[LogicalRelation])//判断是否是
      .map(_.asInstanceOf[LogicalRelation])//转换实例
      .map(_.relation)//取属性
      .toList

    if (relations.isEmpty || relations.exists(!_.isInstanceOf[TiDBRelation])) {
      Nil
    } else {
      val sources = relations.map(_.asInstanceOf[TiDBRelation])//BaseRelation转成TiDBRelation
      val source = sources.head//取list第一个
      doPlan(source, plan)//开始执行plan
    }
  }

  private def toCoprocessorRDD(source: TiDBRelation,
                               output: Seq[Attribute],
                               selectRequest: TiSelectRequest): SparkPlan = {//执行plan转换成rdd
    val schemaTypes = StructType.fromAttributes(output).fields.map(_.dataType)
    selectRequest.setTableInfo(source.table)
    val rdd = source.logicalPlanToRDD(selectRequest)
    val internalRdd = RDDConversions.rowToRowRdd(rdd, schemaTypes)

    RDDScanExec(output, internalRdd, "CoprocessorRDD")
  }

  def projectFilterToSelectRequest(projects: Seq[NamedExpression],
                                   filters: Seq[Expression],
                                   source: TiDBRelation,
                                   selectRequest: TiSelectRequest = new TiSelectRequest): TiSelectRequest = {
    val tiFilters = filters.map(expr => expr match { case BasicExpression(expr) => expr })
    val scanBuilder = new ScanBuilder
    val pkIndex = TiIndexInfo.generateFakePrimaryKeyIndex(source.table)
    val scanPlan = scanBuilder.buildScan(JavaConversions.seqAsJavaList(tiFilters),
      pkIndex, source.table)

    selectRequest.addRanges(scanPlan.getKeyRanges)
    scanPlan.getFilters.toList.map(selectRequest.addWhere)

    selectRequest
  }

  def aggregationToSelectRequest(groupByList: Seq[NamedExpression],
                                 aggregates: Seq[AggregateExpression],
                                 source: TiDBRelation,
                                 selectRequest: TiSelectRequest = new TiSelectRequest): TiSelectRequest = {
    aggregates.foreach(expr =>
      expr match {
        case aggExpr: AggregateExpression =>
          aggExpr.aggregateFunction match {
            case Average(_) =>
              assert(false, "Should never be here")
            case Sum(BasicExpression(arg)) => {
              selectRequest.addAggregate(new TiSum(arg),
                fromSparkType(aggExpr.aggregateFunction.dataType))
            }
            case Count(args) => {
              val tiArgs = args.flatMap(BasicExpression.convertToTiExpr)
              selectRequest.addAggregate(new TiCount(tiArgs: _*))
            }
            case Min(BasicExpression(arg)) =>
              selectRequest.addAggregate(new TiMin(arg))

            case Max(BasicExpression(arg)) =>
              selectRequest.addAggregate(new TiMax(arg))

            case First(BasicExpression(arg), _) =>
              selectRequest.addAggregate(new TiFirst(arg))

            case _ => None
          }
      })
    groupByList.foreach(groupItem =>
      groupItem match {
        case BasicExpression(byExpr) =>
          selectRequest.addGroupByItem(TiByItem.create(byExpr, false))
        case _ => None
      }
    )

    selectRequest
  }

  def aggregationFilter(filters: Seq[Expression],
                        source: TiDBRelation,
                        output: Seq[Attribute],
                        selectRequest: TiSelectRequest = new TiSelectRequest): SparkPlan = {
    toCoprocessorRDD(source, output, filterToSelectRequest(filters, source, selectRequest))
  }

  def filterToSelectRequest(filters: Seq[Expression],
                            source: TiDBRelation,
                            selectRequest: TiSelectRequest = new TiSelectRequest): TiSelectRequest = {
    val tiFilters:Seq[TiExpr] = filters.map(expr => expr match { case BasicExpression(expr) => expr })
    val scanBuilder: ScanBuilder = new ScanBuilder
    val pkIndex: TiIndexInfo = TiIndexInfo.generateFakePrimaryKeyIndex(source.table)
    val scanPlan = scanBuilder.buildScan(JavaConversions.seqAsJavaList(tiFilters),
                                         pkIndex, source.table)

    selectRequest.addRanges(scanPlan.getKeyRanges)
    scanPlan.getFilters.toList.map(selectRequest.addWhere)
    selectRequest
  }

  def pruneFilterProject(projectList: Seq[NamedExpression],
                         filterPredicates: Seq[Expression],
                         source: TiDBRelation,
                         selectRequest: TiSelectRequest = new TiSelectRequest): SparkPlan = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val (pushdownFilters: Seq[Expression],
         residualFilters: Seq[Expression]) =
      filterPredicates.partition(TiUtils.isSupportedFilter)

    val residualFilter: Option[Expression] = residualFilters.reduceLeftOption(catalyst.expressions.And)

    filterToSelectRequest(pushdownFilters, source, selectRequest)

    // Right now we still use a projection even if the only evaluation is applying an alias
    // to a column.  Since this is a no-op, it could be avoided. However, using this
    // optimization with the current implementation would change the output schema.
    // TODO: Decouple final output schema from expression evaluation so this copy can be
    // avoided safely.
    if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
      filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val projectSeq: Seq[Attribute] = projectList.asInstanceOf[Seq[Attribute]]
      projectSeq.foreach(attr => selectRequest.addField(TiColumnRef.create(attr.name)))
      val scan = toCoprocessorRDD(source, projectSeq, selectRequest)
      residualFilter.map(FilterExec(_, scan)).getOrElse(scan)
    } else {
      // for now all column used will be returned for old interface
      // TODO: once switch to new interface we change this pruning logic
      val projectSeq: Seq[Attribute] = (projectSet ++ filterSet).toSeq
      projectSeq.foreach(attr => selectRequest.addField(TiColumnRef.create(attr.name)))
      val scan = toCoprocessorRDD(source, projectSeq, selectRequest)
      ProjectExec(projectList, residualFilter.map(FilterExec(_, scan)).getOrElse(scan))
    }
  }

  // We do through similar logic with original Spark as in SparkStrategies.scala
  // Difference is we need to test if a sub-plan can be consumed all together by TiKV
  // and then we don't return (don't planLater) and plan the remaining all at once
  private def doPlan(source: TiDBRelation, plan: LogicalPlan): Seq[SparkPlan] = {

    val aliasMap = mutable.HashMap[Expression, Alias]()
    val avgPushdownRewriteMap = mutable.HashMap[ExprId, List[AggregateExpression]]()
    val avgFinalRewriteMap = mutable.HashMap[ExprId, List[AggregateExpression]]()

    def toAlias(expr: Expression) = aliasMap.getOrElseUpdate(expr, Alias(expr, expr.toString)())//别名转换

    def newAggregate(aggFunc: AggregateFunction,
                     originalAggExpr: AggregateExpression) =
      AggregateExpression(aggFunc,
                          originalAggExpr.mode,
                          originalAggExpr.isDistinct,
                          originalAggExpr.resultId)//聚合 表达式

    def newAggregateWithId(aggFunc: AggregateFunction,
                           originalAggExpr: AggregateExpression) =
      AggregateExpression(aggFunc,
        originalAggExpr.mode,
        originalAggExpr.isDistinct,
        NamedExpression.newExprId)//聚合 表达式

    // TODO: This test should be done once for all children
    plan match {
      // Collapse filters and projections and push plan directly
        //模式匹配PhysicalOperation的unapply获得　UnaryNode的Project和Filter
      case PhysicalOperation(projectList, filters, LogicalRelation(source: TiDBRelation, _, _)) =>
        pruneFilterProject(projectList, filters, source) :: Nil

      // Basic logic of original Spark's aggregation plan is:
      // PhysicalAggregation extractor will rewrite original aggregation
      // into aggregateExpressions and resultExpressions.
      // resultExpressions contains only references [[AttributeReference]]
      // to the result of aggregation. resultExpressions might contain projections
      // like Add(sumResult, 1).
      // For a aggregate like agg(expr) + 1, the rewrite process is: rewrite agg(expr) ->
      // 1. pushdown: agg(expr) as agg1, if avg then sum(expr), count(expr)
      // 2. residual expr (for Spark itself): agg(agg1) as finalAgg1 the parameter is a
      // reference to pushed plan's corresponding aggregation
      // 3. resultExpressions: finalAgg1 + 1, the finalAgg1 is the reference to final result
      // of the aggregation

      //unapply匹配出UnaryNode的Aggregate

      case PhysicalAggregation(//Seq[NamedExpression], Seq[AggregateExpression], Seq[NamedExpression], LogicalPlan
      groupingExpressions,//NamedExpression group by ..
      aggregateExpressions,//AggregateExpression class,avg(score)
      resultExpressions,//NamedExpression
      TiAggregationProjection(filters, _, source))//对LogicalPlan进行分解
        if !aggregateExpressions.exists(_.isDistinct) =>
        var selReq: TiSelectRequest = filterToSelectRequest(filters, source)
        val residualAggregateExpressions = aggregateExpressions.map {
          aggExpr =>
            aggExpr.aggregateFunction match {
              // here aggExpr is the original AggregationExpression
              // and will be pushed down to TiKV
              case Max(_) => newAggregate(Max(toAlias(aggExpr).toAttribute), aggExpr)//根据 别名，旧的表达式 构造新的表达式，下推
              case Min(_) => newAggregate(Min(toAlias(aggExpr).toAttribute), aggExpr)
              case Count(_) => newAggregate(Sum(toAlias(aggExpr).toAttribute), aggExpr)
              case Sum(_) => newAggregate(Sum(toAlias(aggExpr).toAttribute), aggExpr)
              case First(_, ignoreNullsExpr) =>
                newAggregate(First(toAlias(aggExpr).toAttribute, ignoreNullsExpr), aggExpr)
              case _ => aggExpr
            }
        } flatMap {//先映射再扁平化
          aggExpr =>
            aggExpr match {
              // We have to separate average into sum and count
              // and for outside expression such as average(x) + 1,
              // Spark has lift agg + 1 up to resultExpressions
              // We need to modify the reference there as well to forge
              // Divide(sum/count) + 1
              //模式匹配 求平均的表达式
              case aggExpr@AggregateExpression(Average(ref), _, _, _) =>
                // Need a type promotion
                val promotedType = ref.dataType match {//转换类型为double用来计算
                  case DoubleType | DecimalType.Fixed(_, _) | LongType => ref
                  case _ => Cast(ref, DoubleType)//其他数据类型 转换成double
                }
                val sumToPush = newAggregate(Sum(promotedType), aggExpr)//表达式下推给行的子表达式
                val countToPush = newAggregate(Count(ref), aggExpr)

                // Need a new expression id since they are not simply rewrite as above
                val sumFinal = newAggregateWithId(Sum(toAlias(sumToPush).toAttribute), aggExpr)//全部子表达式加起来
                val countFinal = newAggregateWithId(Sum(toAlias(countToPush).toAttribute), aggExpr)

                avgPushdownRewriteMap(aggExpr.resultId) = List(sumToPush, countToPush)
                avgFinalRewriteMap(aggExpr.resultId) = List(sumFinal, countFinal)
                List(sumFinal, countFinal)//返回list给flatMap
              case _ => aggExpr :: Nil
            }
        }

        val pushdownAggregates = aggregateExpressions.flatMap {//聚合下推
          aggExpr =>
            avgPushdownRewriteMap
              .getOrElse(aggExpr.resultId, List(aggExpr))
        }

        selReq = aggregationToSelectRequest(groupingExpressions,
          pushdownAggregates,
          source,
          selReq)

        val rewrittenResultExpression = resultExpressions.map(//重写结果表达式
          expr => expr.transformDown {
            case aggExpr: AttributeReference
              if avgFinalRewriteMap.contains(aggExpr.exprId) =>
              // Replace the original Average expression with Div of Alias
              val sumCountPair = avgFinalRewriteMap(aggExpr.exprId)

              // We missed the chance for auto-coerce already
              // so manual cast needed
              // Also, convert into resultAttribute since
              // they are created by tiSpark without Spark conversion
              // TODO: Is DoubleType a best target type for all?
              Cast(
                Divide(//两个结果分别求和并相除
                  Cast(sumCountPair.head.resultAttribute, DoubleType),
                  Cast(sumCountPair(1).resultAttribute, DoubleType)
                ),
                aggExpr.dataType
              )
            case other => other
          }.asInstanceOf[NamedExpression]
        )

        val output = (groupingExpressions ++ pushdownAggregates.map(x => toAlias(x))).map(_.toAttribute)

        aggregate.AggUtils.planAggregateWithoutDistinct(
          groupingExpressions,
          residualAggregateExpressions,
          rewrittenResultExpression,
          toCoprocessorRDD(source, output, selReq))

      case _ => Nil
    }
  }
}


object TiAggregationProjection {
  type ReturnType = (Seq[Expression], LogicalPlan, TiDBRelation)

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    plan match {
      // Only push down aggregates projection when all filters can be applied and
      // all projection expressions are column references
        // rel@LogicalRelation 匹配source为TiDBRelation的 LogicalRelation
      case PhysicalOperation(projects, filters, rel@LogicalRelation(source: TiDBRelation, _, _))
        if projects.forall(_.isInstanceOf[Attribute]) &&
          filters.forall(TiUtils.isSupportedFilter) =>
        Some((filters, rel, source))
      case _ => Option.empty[ReturnType]
    }
  }
}