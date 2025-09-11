package io.github.amrnablus.sparklineage.utils

import io.github.amrnablus.sparklineage.transport.TransportTrait
import org.apache.spark.SparkConf

class LineageConfig(sparkConf: SparkConf) {

  private val BASE_PREFIX = "io.github.amrnablus.sparklineage"

  private val allConfigs: Map[String, String] = sparkConf.getAll
    .filter { case (k, _) => k.startsWith(BASE_PREFIX) }
    .map { case (k, v) => (k.stripPrefix(s"$BASE_PREFIX."), v) }
    .toMap

  val transportType: String = allConfigs.getOrElse(
    "transport.type",
    throw new IllegalArgumentException(
      s"$BASE_PREFIX.transport.type is required"
    )
  )

  val transportClass: String = allConfigs.getOrElse(
    "transport.class",
    throw new IllegalArgumentException(
      s"$BASE_PREFIX.transport.class is required"
    )
  )

  private val transportPrefix = s"transport.$transportType."
  val transportConfigs: Map[String, String] = allConfigs
    .filter { case (k, _) => k.startsWith(transportPrefix) }
    .map { case (k, v) => (k.stripPrefix(transportPrefix), v) }

  val transport: TransportTrait =
    try {
      val clazz = Class.forName(transportClass)
      val constructor = clazz.getConstructor(classOf[Map[String, String]])
      constructor.newInstance(transportConfigs).asInstanceOf[TransportTrait]
    } catch {
      case _: ClassNotFoundException =>
        throw new RuntimeException(
          s"Transport class '$transportClass' not found"
        )
      case e: NoSuchMethodException =>
        throw new RuntimeException(
          s"Transport class '$transportClass' must have a constructor accepting Map[String,String]",
          e
        )
      case e: Exception =>
        throw new RuntimeException(
          s"Failed to instantiate transport class '$transportClass'",
          e
        )
    }

}
