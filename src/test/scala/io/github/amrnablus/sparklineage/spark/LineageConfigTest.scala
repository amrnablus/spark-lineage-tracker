package io.github.amrnablus.sparklineage.spark

import io.github.amrnablus.sparklineage.transport.TransportTrait
import io.github.amrnablus.sparklineage.utils.LineageConfig
import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite

class DummyTransport(config: Map[String, String]) extends TransportTrait {
  override def trackLineage(
      destinationTables: PlanParser.TableInfo,
      sourceTables: Seq[PlanParser.TableInfo],
      sql: String
  ): Unit = {
    if (config.contains("fail")) {}
  }
}

class LineageConfigTest extends AnyFunSuite {

  test("should extract configs and instantiate transport") {
    val conf = new SparkConf()
      .set("io.github.amrnablus.sparklineage.transport.type", "dummy")
      .set(
        "io.github.amrnablus.sparklineage.transport.class",
        classOf[DummyTransport].getName
      )
      .set("io.github.amrnablus.sparklineage.transport.dummy.key", "value")

    val lineageConfig = new LineageConfig(conf)
    assert(lineageConfig.transportType == "dummy")
    assert(lineageConfig.transportClass == classOf[DummyTransport].getName)
    assert(lineageConfig.transportConfigs("key") == "value")
    assert(lineageConfig.transport.isInstanceOf[DummyTransport])
  }

  test("should throw if transport.type is missing") {
    val conf = new SparkConf()
      .set(
        "io.github.amrnablus.sparklineage.transport.class",
        classOf[DummyTransport].getName
      )
    assertThrows[IllegalArgumentException] {
      new LineageConfig(conf)
    }
  }

  test("should throw if transport.class is missing") {
    val conf = new SparkConf()
      .set("io.github.amrnablus.sparklineage.transport.type", "dummy")
    assertThrows[IllegalArgumentException] {
      new LineageConfig(conf)
    }
  }

  test("should throw if transport class not found") {
    val conf = new SparkConf()
      .set("io.github.amrnablus.sparklineage.transport.type", "dummy")
      .set(
        "io.github.amrnablus.sparklineage.transport.class",
        "non.existent.Class"
      )
    assertThrows[RuntimeException] {
      new LineageConfig(conf)
    }
  }

  test("should throw if transport class constructor is invalid") {
    class InvalidTransport()
    val conf = new SparkConf()
      .set("io.github.amrnablus.sparklineage.transport.type", "dummy")
      .set(
        "io.github.amrnablus.sparklineage.transport.class",
        classOf[InvalidTransport].getName
      )
    assertThrows[RuntimeException] {
      new LineageConfig(conf)
    }
  }
}
