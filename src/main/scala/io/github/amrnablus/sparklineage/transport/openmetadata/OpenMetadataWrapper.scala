package io.github.amrnablus.sparklineage.transport.openmetadata

import com.typesafe.scalalogging.Logger
import io.circe.Printer
import io.circe.generic.auto._
import io.github.amrnablus.sparklineage.spark.PlanParser.TableInfo
import sttp.client4._
import sttp.client4.httpurlconnection.HttpURLConnectionBackend

import scala.collection.mutable

class OpenMetadataWrapper(baseUrl: String, apiKey: String) {
  private val logger: Logger = Logger(getClass.getName)

  private val backend = HttpURLConnectionBackend()

  case class Database(name: String)

  case class Table(id: String, name: String, database: Database)

  private case class TableResponse(data: List[Table], paging: Option[Paging])

  private case class Paging(
      before: Option[String],
      after: Option[String],
      total: Int
  )

  def loadTables(tableInfo: Seq[TableInfo]): Map[TableInfo, String] = {
    val allTables = tableInfo.size

    val response = getTables(Option.empty)
    var afterExists: Boolean =
      response.body.isRight &&
        response.body.right.get.paging.isDefined &&
        response.body.right.get.paging.get.after.isDefined &&
        response.body.right.get.paging.get.after.get != ""

    var after =
      if (afterExists) Option(response.body.right.get.paging.get.after.get)
      else Option.empty

    val tableMap = extractTablesFromResponse(response, tableInfo)

    while (tableMap.size < allTables && afterExists) {
      val response = getTables(after)
      afterExists =
        response.body.isRight && response.body.right.get.paging.isDefined && response.body.right.get.paging.get.after.isDefined && response.body.right.get.paging.get.after.get != ""
      tableMap ++= extractTablesFromResponse(response, tableInfo)
      after =
        if (afterExists) Option(response.body.right.get.paging.get.after.get)
        else Option.empty

    }
    println()

    tableMap.toMap
  }

  private def extractTablesFromResponse(
      response: Response[Either[ResponseException[String], TableResponse]],
      tablesAndSchemas: Seq[TableInfo]
  ): mutable.Map[TableInfo, String] = {
    val result = mutable.Map[TableInfo, String]()

    response.body match {
      case Right(tableResponse) =>
        tableResponse.data.foreach(table => {
          val maybeTable = tablesAndSchemas.find(info =>
            info.name == table.name && info.database == table.database.name
          )
          if (maybeTable.isDefined) {
            result.put(maybeTable.get, table.id)
          }
        })

      case Left(error) =>
        // TODO: Decide whether to Break the entire listener or just skip this call
        logger.error(
          f"Did not get a proper response from OpenMetadata API: ${error}"
        )
    }
    result
  }

  /** Why does this function exist? Because Open Metadata API does not allow
    * searching for tables using wildcards, therefore, the options are either
    * using the table's FQDN to get the exact table id, or searching for tables
    * manually like I'm doing now. I opted for this for the sake of developer's
    * convenience
    */
  private def getTables(
      after: Option[String]
  ): Response[Either[ResponseException[String], TableResponse]] = {
    val request = basicRequest
      .get(uri"$baseUrl/tables?limit=150&after=${after.getOrElse("")}")
      .header("Authorization", s"Bearer $apiKey")
      .response(circe.asJson[TableResponse])

    request.send(backend)
  }

  def createLineage(
      sourceIds: Seq[String],
      targetId: String,
      sql: String
  ): Seq[(Int, String)] = {
    val responses = new mutable.ListBuffer[(Int, String)]()
    sourceIds.foreach(sourceId => {
      val fromEntity = Entity(id = sourceId, `type` = "table")
      val toEntity = Entity(id = targetId, `type` = "table")
      val lineageDetails = LineageDetails(sqlQuery = Option(sql))
      val edge = EdgeWrapper(
        Edge(
          fromEntity = fromEntity,
          lineageDetails = lineageDetails,
          toEntity = toEntity
        )
      )

      val printer: Printer = Printer.noSpaces.copy(dropNullValues = true)

      val lineageJsonString =
        printer.print(io.circe.syntax.EncoderOps(edge).asJson)

      val request = basicRequest
        .put(uri"$baseUrl/lineage")
        .header("Authorization", s"Bearer $apiKey")
        .header("Content-Type", "application/json")
        .body(lineageJsonString)
        .response(asString)

      val response = request.send(backend)
      responses += Tuple2(response.code.code, response.body.getOrElse(""))
    })

    responses
  }

}
