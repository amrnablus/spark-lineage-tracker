package io.github.amrnablus.sparklineage.spark

import io.github.amrnablus.sparklineage.spark.PlanParser.{
  TableInfo,
  generateCleanSQL
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{
  UnresolvedAttribute,
  UnresolvedRelation
}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  EqualTo,
  Literal
}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ListBuffer

class PlanParserTest extends AnyFunSuite {

  test("UnresolvedRelation should return table name") {
    val plan = UnresolvedRelation(Seq("my_table"))
    val tablesStack = new ListBuffer[TableInfo]()
    assert(generateCleanSQL(plan, tablesStack) == "my_table")
  }

  test("Project should generate SELECT statement") {
    val child = UnresolvedRelation(Seq("people"))
    val plan = Project(Seq('name.attr, 'age.attr), child)
    val tablesStack = new ListBuffer[TableInfo]()
    assert(
      generateCleanSQL(plan, tablesStack) == "SELECT name, age FROM people"
    )
  }

  test("Filter should append WHERE clause") {
    val child = UnresolvedRelation(Seq("people"))
    val plan = Filter('age > Literal(18), child)
    val tablesStack = new ListBuffer[TableInfo]()
    assert(generateCleanSQL(plan, tablesStack) == "people WHERE (age > 18)")
  }

  test("Join should include ON clause") {
    val left = UnresolvedRelation(Seq("people"))
    val right = UnresolvedRelation(Seq("departments"))

    val condExpr = EqualTo(
      functions.col("people.dept_id").expr,
      functions.col("departments.id").expr
    )
    val plan = Join(
      left,
      right,
      joinType = Inner,
      condition = Some(condExpr),
      JoinHint(Option.empty, Option.empty)
    )

    val tablesStack = new ListBuffer[TableInfo]()
    val sql = generateCleanSQL(plan, tablesStack)
    assert(sql.contains("INNER JOIN"))
    assert(sql.contains("ON (people.dept_id = departments.id)"))

  }

  test("Aggregate should wrap in parentheses with GROUP BY") {
    val child = UnresolvedRelation(Seq("sales"))
    // grouping expressions can stay as UnresolvedAttribute
    val groupingExprs = Seq(UnresolvedAttribute("region"))

    // aggregate expressions must be NamedExpression
    val aggregateExprs = Seq(
      Alias(UnresolvedAttribute("region"), "region")(),
      Alias(Sum(UnresolvedAttribute("amount")), "sum(amount)")()
    )

    val plan = Aggregate(groupingExprs, aggregateExprs, child)
    val tablesStack = new ListBuffer[TableInfo]()

    val sql = generateCleanSQL(plan, tablesStack)
    assert(sql.contains("GROUP BY region"))
    assert(sql.contains("sum(amount)"))

  }

  test("Sort should append ORDER BY") {
    val child = UnresolvedRelation(Seq("people"))
    val plan = Sort(Seq('age.asc), global = true, child)
    val tablesStack = new ListBuffer[TableInfo]()

    assert(
      generateCleanSQL(
        plan,
        tablesStack
      ) == "people ORDER BY age ASC NULLS FIRST"
    )
  }

  test("SubqueryAlias should wrap in parentheses") {
    val child = UnresolvedRelation(Seq("employees"))
    val plan = SubqueryAlias("e", child)
    val tablesStack = new ListBuffer[TableInfo]()

    val sql = generateCleanSQL(plan, tablesStack)
    assert(sql == "(employees) AS e")
  }

  test("GlobalLimit should append LIMIT") {
    val child = UnresolvedRelation(Seq("people"))
    val plan = GlobalLimit(Literal(10), child)
    val tablesStack = new ListBuffer[TableInfo]()
    assert(generateCleanSQL(plan, tablesStack) == "people LIMIT 10")
  }

  test("LocalRelation should produce SQL representation") {
    val schema = StructType(Seq(StructField("id", IntegerType)))

    // convert StructType to Seq[AttributeReference]
    val attributes = schema.map { field =>
      AttributeReference(
        field.name,
        field.dataType,
        nullable = field.nullable
      )()
    }

    // create LocalRelation
    val localRel = LocalRelation(attributes, data = Seq.empty[InternalRow])
    val tablesStack = new ListBuffer[TableInfo]()

    val sql = generateCleanSQL(localRel, tablesStack)
    assert(sql.contains("__SPARK_LOCAL_RELATION"))
  }

  test("Fallback should call toString") {
    val customPlan = OneRowRelation()
    val tablesStack = new ListBuffer[TableInfo]()

    val sql = generateCleanSQL(customPlan, tablesStack)
    assert(sql.contains("SELECT 1"))
  }

  test("Aggregate with nested subquery") {
    val innerAgg = Aggregate(
      Seq(UnresolvedAttribute("dept")),
      Seq(
        Alias(UnresolvedAttribute("dept"), "dept")(),
        Alias(Sum(UnresolvedAttribute("salary")), "total_salary")()
      ),
      UnresolvedRelation(Seq("employees"))
    )
    val plan = SubqueryAlias("agg_sub", innerAgg)
    val tablesStack = new ListBuffer[TableInfo]()

    val sql = generateCleanSQL(plan, tablesStack)
    assert(sql.contains("GROUP BY dept"))
    assert(
      sql.contains("\n(\nSELECT dept AS dept, sum(salary) AS total_salary")
    )
  }
}
