package io.github.amrnablus.sparklineage.transport.openmetadata

case class EdgeWrapper(edge: Edge)

case class Edge(
    description: String = "",
    fromEntity: Entity,
    lineageDetails: LineageDetails,
    toEntity: Entity
)

case class Entity(
    deleted: Boolean = false,
    description: Option[String] = Option.empty,
    displayName: Option[String] = Option.empty,
    fullyQualifiedName: Option[String] = Option.empty,
    href: Option[String] = Option.empty,
    id: String,
    inherited: Boolean = false,
    name: Option[String] = Option.empty,
    `type`: String = ""
)

case class LineageDetails(
    columnsLineage: Option[Seq[ColumnLineage]] = Option.empty,
    description: Option[String] = Option.empty,
    pipeline: Option[Entity] = Option.empty,
    source: Option[String] = Option.empty,
    sqlQuery: Option[String] = Option.empty
)

case class ColumnLineage(
    fromColumns: Seq[String],
    function: String,
    toColumn: String
)
