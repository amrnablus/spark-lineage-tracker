package io.github.amrnablus.sparklineage.transport.openmetadata

import io.github.amrnablus.sparklineage.spark.PlanParser
import io.github.amrnablus.sparklineage.transport.TransportTrait

class OpenMetadataLineageClient(config: Map[String, String])
    extends TransportTrait {
  private val apiUrl = requireParam(config, "apiUrl")
  private val apiKey = requireParam(config, "apiKey")
  private val openMetadataWrapper =
    new OpenMetadataWrapper(baseUrl = apiUrl, apiKey = apiKey)

  override def trackLineage(
      destinationTable: PlanParser.TableInfo,
      sourceTables: Seq[PlanParser.TableInfo],
      sql: String
  ): Unit = {
    val tableIds =
      openMetadataWrapper.loadTables(sourceTables :+ destinationTable)
    openMetadataWrapper.createLineage(
      tableIds.filter(_._2 != tableIds(destinationTable)).values.toList,
      tableIds(destinationTable),
      sql
    )
  }
}
