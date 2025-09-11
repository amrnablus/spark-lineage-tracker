package io.github.amrnablus.sparklineage.utils

import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import java.net.URI
import java.util.Objects

object JdbcTools {

  private case class ParsedUrl(
      scheme: String,
      host: String,
      port: Int,
      database: String,
      query: Option[String]
  )

  private def parseJdbcUrl(databaseUrl: String): ParsedUrl = {
    val canonicalUrl =
      if (databaseUrl.startsWith("jdbc:")) {
        databaseUrl.substring("jdbc:".length)
      } else databaseUrl

    val uri = URI.create(
      Objects.requireNonNull(canonicalUrl, "'databaseUrl' must not be null.")
    )
    val path = uri.getPath
    val database = if (path.startsWith("/")) path.substring(1) else path
    val query = Option(uri.getQuery)

    ParsedUrl(
      scheme = uri.getScheme,
      host = uri.getHost,
      port = uri.getPort,
      database = database,
      query = query
    )
  }

  def getCanonicalTableName(tableName: String, jdbcURL: String): String = {
    // Remove quotes for easier parsing
    val unquoted = tableName.replaceAll("^[`\"]|[`\"]$", "")

    // Split on dot
    val parts = unquoted.split("\\.", 2)

    val canonicalName = parts.length match {
      case 1 =>
        val parsedUrl = parseJdbcUrl(jdbcURL)
        // Only table name provided, prepend schema/database
        s"${parsedUrl.database}.${parts(0)}"
      case 2 =>
        // Already has schema/table, keep as-is
        s"${parts(0)}.${parts(1)}"
    }

    // Re-add quotes if original had them
    val needsBackticks = tableName.startsWith("`") || tableName.startsWith("\"")
    if (needsBackticks) s"`$canonicalName`" else canonicalName
  }

  def extractJDBCOptions(lr: LogicalRelation): Option[JDBCOptions] = {
    val rel = lr.relation
    val jdbcOptionsFieldOpt =
      lr.relation.getClass.getDeclaredFields.find(_.getName == "jdbcOptions")
    jdbcOptionsFieldOpt match {
      case Some(jf) =>
        jf.setAccessible(true)
        Option(jf.get(rel).asInstanceOf[JDBCOptions])
      case None => Option.empty
    }
  }
}
