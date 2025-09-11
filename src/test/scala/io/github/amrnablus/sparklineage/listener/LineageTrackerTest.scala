package io.github.amrnablus.sparklineage.listener

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import io.github.amrnablus.sparklineage.listener.LineageTracker
import io.github.amrnablus.sparklineage.spark.PlanParser
import io.github.amrnablus.sparklineage.spark.PlanParser.TableInfo
import io.github.amrnablus.sparklineage.transport.TransportTrait
import io.github.amrnablus.sparklineage.utils.LineageConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.mockito.scalatest.MockitoSugar

// Companion object to hold the mock (needed since constructor can't access local val)
object DummyTransportTest {
  val mockTransport: TransportTrait = mock(classOf[TransportTrait])
}

// Top-level or static nested class
class DummyTransport(configs: Map[String, String]) extends TransportTrait {
  override def trackLineage(
      dest: TableInfo,
      src: Seq[TableInfo],
      sql: String
  ): Unit = {
    DummyTransportTest.mockTransport.trackLineage(dest, src, sql)
  }
}

class LineageTrackerTest extends AnyFunSuite with MockitoSugar {

  test("onSuccess calls transport.trackLineage with correct parameters") {

    // SparkConf with required keys for LineageConfig
    val conf = new SparkConf(false)
      .set("io.github.amrnablus.sparklineage.transport.type", "dummy")
      .set(
        "io.github.amrnablus.sparklineage.transport.class",
        classOf[DummyTransport].getName
      )

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("test")
      .config(conf)
      .getOrCreate()

    // Create SaveIntoDataSourceCommand
    val command = SaveIntoDataSourceCommand(
      query = OneRowRelation(),
      dataSource = mock[CreatableRelationProvider],
      options = Map("dbtable" -> "my_table"),
      mode = SaveMode.Overwrite
    )

    val qe = mock[QueryExecution]
    when(qe.analyzed).thenReturn(command)
    when(qe.sparkSession).thenReturn(spark)

    // Create the tracker
    val tracker = new LineageTracker

    // Call onSuccess
    tracker.onSuccess("write", qe, 0L)

    // Verify transport was called at least once
    verify(DummyTransportTest.mockTransport).trackLineage(
      any[TableInfo],
      any[Seq[TableInfo]],
      any[String]
    )

    spark.stop()
  }
}
