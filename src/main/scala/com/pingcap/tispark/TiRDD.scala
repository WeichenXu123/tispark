<<<<<<< HEAD
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

package com.pingcap.tispark

import com.pingcap.tikv._
import com.pingcap.tikv.meta.{TiSelectRequest, TiTableInfo}
import com.pingcap.tikv.operation.SchemaInfer
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.types.DataType
import com.pingcap.tikv.util.RangeSplitter
=======
package com.pingcap.tispark

import com.google.proto4pingcap.ByteString
import com.pingcap.tidb.tipb.SelectRequest
import com.pingcap.tikv.catalog.Catalog
import com.pingcap.tikv.meta.{TiDBInfo, TiRange, TiTableInfo}
import com.pingcap.tikv.util.RangeSplitter
import com.pingcap.tikv.{Snapshot, TiCluster, TiConfiguration}
>>>>>>> 261a30a9ccd7c9969cd06666a150e9e9dd860667
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

<<<<<<< HEAD
import scala.collection.JavaConversions._


class TiRDD(selectReq: TiSelectRequest, sc: SparkContext, options: TiOptions, table: TiTableInfo)
  extends RDD[Row](sc, Nil) {

  type TiRow = com.pingcap.tikv.row.Row

  @transient lazy val meta: MetaManager = new MetaManager(options.addresses)
  @transient lazy val cluster: TiCluster = meta.cluster
  @transient lazy val snapshot: Snapshot = cluster.createSnapshot()
  @transient lazy val (fieldsType: List[DataType], rowTransformer: RowTransformer) = initializeSchema

  def initializeSchema(): (List[DataType], RowTransformer) = {
    val schemaInferrer: SchemaInfer = SchemaInfer.create(selectReq)
    val rowTransformer: RowTransformer = schemaInferrer.getRowTransformer
    (schemaInferrer.getTypes.toList, rowTransformer)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    context.addTaskCompletionListener{ _ => cluster.close() }

    selectReq.bind
    // bypass, sum return a long type
    val tiPartition = split.asInstanceOf[TiPartition]
    val iterator = snapshot.select(selectReq, split.asInstanceOf[TiPartition].task)
    val finalTypes = rowTransformer.getTypes.toList

    def toSparkRow(row: TiRow): Row = {
      val transRow = rowTransformer.transform(row)
      val rowArray = new Array[Any](rowTransformer.getTypes.size)

      for (i <- 0 until transRow.fieldCount) {
        rowArray(i) = transRow.get(i, finalTypes(i))
      }

      Row.fromSeq(rowArray)
    }

    override def hasNext: Boolean = iterator.hasNext

    override def next(): Row = toSparkRow(iterator.next)
  }

  override protected def getPartitions: Array[Partition] = {
    val keyWithRegionTasks = RangeSplitter.newSplitter(cluster.getRegionManager)
                 .splitRangeByRegion(selectReq.getRanges)

    keyWithRegionTasks.zipWithIndex.map{
      case (task, index) => new TiPartition(index, task)
=======
import scala.collection.JavaConverters._


class TiRDD(selectRequestInBytes: ByteString, ranges: List[TiRange[java.lang.Long]], sc: SparkContext, options: TiOptions)
  extends RDD[Row](sc, Nil) {

  @transient var tiConf: TiConfiguration = null
  @transient var cluster: TiCluster = null
  @transient var catalog: Catalog = null
  @transient var database: TiDBInfo = null
  @transient var table: TiTableInfo = null
  @transient var snapshot: Snapshot = null
  @transient var selectReq: SelectRequest = null

  init()

  def init(): Unit = {
    tiConf = TiConfiguration.createDefault(options.addresses.asJava)
    cluster = TiCluster.getCluster(tiConf)
    catalog = cluster.getCatalog
    database = catalog.getDatabase(options.databaseName)
    table = catalog.getTable(database, options.tableName)
    snapshot = cluster.createSnapshot()
    selectReq = SelectRequest.parseFrom(selectRequestInBytes)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    init()
    context.addTaskCompletionListener{ _ => cluster.close() }

    val tiPartition = split.asInstanceOf[TiPartition]
    val iterator = snapshot.select(selectReq, tiPartition.region, tiPartition.store, tiPartition.tiRange)
    def toSparkRow(row: com.pingcap.tikv.meta.Row): Row = {
      val rowArray = new Array[Any](row.fieldCount())
      for (i <- 0 until row.fieldCount()) {
        rowArray(i) = row.get(i, table.getColumns.get(i).getType)
      }
      Row.fromSeq(rowArray)
    }

    override def hasNext: Boolean = {
      iterator.hasNext
    }

    override def next(): Row = {
      toSparkRow(iterator.next)
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val keyRanges = Snapshot.convertHandleRangeToKeyRange(table, ranges.asJava)
    val keyWithRegionRanges = RangeSplitter.newSplitter(cluster.getRegionManager)
                 .splitRangeByRegion(keyRanges)
    keyWithRegionRanges.asScala.zipWithIndex.map{
      case (keyRegionPair, index) => new TiPartition(index,
                                            keyRegionPair.first.first, /* Region */
                                            keyRegionPair.first.second, /* Store */
                                            keyRegionPair.second) /* Range */
>>>>>>> 261a30a9ccd7c9969cd06666a150e9e9dd860667
    }.toArray
  }
}
