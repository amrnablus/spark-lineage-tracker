package io.github.amrnablus.sparklineage.spark

import com.typesafe.scalalogging.Logger
import io.github.amrnablus.sparklineage.utils.JdbcTools
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{
  LogicalRelation,
  SaveIntoDataSourceCommand
}

import scala.collection.mutable.ListBuffer

object PlanParser {
  private val logger: Logger = Logger(getClass.getName)

  case class TableInfo(
      name: String,
      canonicalName: String,
      database: String,
      schema: Option[String] = Option.empty
  )

  private val SQL_DISCLAIMER_COMMENT =
    "\n-- THIS SQL IS EXTRACTED FROM SPARK AND IS NOT INTENDED TO RUN IN A PRODUCTION ENVIRONMENT\n"

  def generateSql(
      command: SaveIntoDataSourceCommand
  ): (String, TableInfo, Seq[TableInfo]) = {
    val tableStack = ListBuffer[TableInfo]()
    command.options.get("dbtable") match {
      case Some(name) =>
        val destinationTableParts =
          JdbcTools
            .getCanonicalTableName(name, command.options.getOrElse("url", ""))
            .split("\\.")

        if (destinationTableParts.size >= 2) {
          val destinationTableInfo = TableInfo(
            name = destinationTableParts(1),
            database = destinationTableParts(0),
            schema =
              if (destinationTableParts.size >= 3)
                Option(destinationTableParts(2))
              else Option.empty,
            canonicalName = name
          )

          (
            SQL_DISCLAIMER_COMMENT + generateCleanSQL(
              command.query,
              tableStack
            ),
            destinationTableInfo,
            tableStack
          )
        } else {
          logger.warn("Writing to an unknow table, skipping lineage")
          throw new RuntimeException(
            "Writing to an unknow table, skipping lineage"
          )
        }

      case _ =>
        logger.warn(
          "dbtable not present in the write query, cannot save lineage"
        )
        throw new RuntimeException(
          "dbtable not present in the write query, cannot save lineage"
        )
    }
  }

  def generateCleanSQL(
      plan: LogicalPlan,
      tableStack: ListBuffer[TableInfo]
  ): String = plan match {
    case p: Project =>
      s"SELECT ${p.projectList.map(_.sql).mkString(", ")} FROM ${generateCleanSQL(p.child, tableStack)}"

    case f: Filter =>
      s"${generateCleanSQL(f.child, tableStack)} WHERE ${f.condition.sql}"

    case j: Join =>
      val joinType = j.joinType.sql.replace("`", "")
      s"${generateCleanSQL(j.left, tableStack)} $joinType JOIN ${generateCleanSQL(j.right, tableStack)}" +
        s"\nON ${j.condition.map(_.sql).getOrElse("")}"

    case a: Aggregate =>
      s"\n(\nSELECT ${a.aggregateExpressions.map(_.sql).mkString(", ")} " +
        s"FROM ${generateCleanSQL(a.child, tableStack)} " +
        s"GROUP BY ${a.groupingExpressions.map(_.sql).mkString(", ")}\n)\n"

    case so: Sort =>
      s"${generateCleanSQL(so.child, tableStack)} ORDER BY ${so.order.map(_.sql).mkString(", ")}"

    case s: SubqueryAlias =>
      s"(${generateCleanSQL(s.child, tableStack)}) AS ${s.alias}"

    case l: GlobalLimit =>
      s"${generateCleanSQL(l.child, tableStack)} LIMIT ${l.limitExpr}"

    case u: UnresolvedRelation =>
      u.tableName

    case lr: LogicalRelation if lr.relation.toString.contains("JDBCRelation") =>
      val tableInfo = extractTableInfoFromRelation(lr)
      tableStack += tableInfo
      tableInfo.canonicalName
    case _: LocalRelation =>
      "__SPARK_LOCAL_RELATION"

    case Union(children, _, _) =>
      children
        .map(p => s"(${generateCleanSQL(p, tableStack)})")
        .mkString(" UNION ALL ")

    // A relation with just one empty row
    case OneRowRelation() =>
      "(SELECT 1 AS placeholder_col LIMIT 0)" // Simulate an empty relation

    // Fallback
    case other =>
      other.toString
  }

  private def extractTableInfoFromRelation(lr: LogicalRelation): TableInfo = {
    val jdbcOptions = JdbcTools.extractJDBCOptions(lr)
    val tableName =
      lr.relation.toString.replaceAll(".*JDBCRelation\\(([\\w\\.]+)\\).*", "$1")

    val fullTableName =
      JdbcTools.getCanonicalTableName(tableName, jdbcOptions.get.url)
    val tableInfo = fullTableName.replace("`", "").split("\\.")
    if (tableInfo.length < 2) {
      throw new RuntimeException("Invalid table name: $fullTableName")
    }
    TableInfo(
      name = tableInfo(1),
      canonicalName = fullTableName,
      database = tableInfo(0),
      schema = if (tableInfo.size >= 3) Option(tableInfo(2)) else Option.empty
    )
  }

}
