package io.github.amrnablus.sparklineage.listener

import com.typesafe.scalalogging.Logger
import io.github.amrnablus.sparklineage.spark.PlanParser
import io.github.amrnablus.sparklineage.utils.LineageConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import org.apache.spark.sql.execution.{ExtendedMode, QueryExecution}
import org.apache.spark.sql.util.QueryExecutionListener

class LineageTracker extends QueryExecutionListener {

  private val logger: Logger = Logger(getClass.getName)
  override def onSuccess(
      funcName: String,
      qe: QueryExecution,
      durationNs: Long
  ): Unit = {
    qe.analyzed match {
      case command: SaveIntoDataSourceCommand =>
        command.options.get("dbtable") match {
          case Some(dbtable: String) =>
            val config = new LineageConfig(qe.sparkSession.sparkContext.getConf)
            logger.info(s"Tracing lineage for a write to $dbtable")
            val (sql, descTable, srcTables) = PlanParser.generateSql(command)
            logger.warn(s"Reconstructed SQL:\n$sql")
            logger.info(s"Callstack: {$descTable ++ $srcTables}")
            config.transport.trackLineage(descTable, srcTables, sql)
          case _ =>
            logger.error("Not writing to a database table, skipping lineage")
        }

      case _ =>
    }

  }

  override def onFailure(
      funcName: String,
      qe: QueryExecution,
      exception: Exception
  ): Unit = {
    // no need to implement
  }

}
