package io.github.amrnablus.sparklineage.transport

import io.github.amrnablus.sparklineage.spark.PlanParser.TableInfo

trait TransportTrait {
  protected def requireParam(params: Map[String, String], key: String): String =
    params.getOrElse(
      key,
      throw new IllegalArgumentException(s"Transport config '$key' is required")
    )

  def trackLineage(
      destinationTables: TableInfo,
      sourceTables: Seq[TableInfo],
      sql: String
  ): Unit
}
