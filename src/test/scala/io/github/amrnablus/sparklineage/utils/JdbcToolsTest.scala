package io.github.amrnablus.sparklineage.utils

import org.scalatest.funsuite.AnyFunSuite

class JdbcToolsTest extends AnyFunSuite {

  test("parseJdbcUrl should parse typical JDBC URL") {
    val url = "jdbc:mysql://localhost:3306/mydb?user=root"
    val parsed =
      JdbcTools.getClass.getDeclaredMethod("parseJdbcUrl", classOf[String])
    parsed.setAccessible(true)
    val result = parsed.invoke(JdbcTools, url).asInstanceOf[Object]
    val scheme = result.getClass.getDeclaredField("scheme")
    scheme.setAccessible(true)
    assert(scheme.get(result) == "mysql")
    val host = result.getClass.getDeclaredField("host")
    host.setAccessible(true)
    assert(host.get(result) == "localhost")
    val port = result.getClass.getDeclaredField("port")
    port.setAccessible(true)
    assert(port.get(result) == 3306)
    val database = result.getClass.getDeclaredField("database")
    database.setAccessible(true)
    assert(database.get(result) == "mydb")
    val query = result.getClass.getDeclaredField("query")
    query.setAccessible(true)
    assert(query.get(result).asInstanceOf[Option[String]].contains("user=root"))
  }

  test(
    "getCanonicalTableName should prepend database if only table name is given"
  ) {
    val jdbcURL = "jdbc:postgresql://host:5432/dbname"
    val tableName = "mytable"
    val result = JdbcTools.getCanonicalTableName(tableName, jdbcURL)
    assert(result == "dbname.mytable")
  }

  test("getCanonicalTableName should keep schema.table if provided") {
    val jdbcURL = "jdbc:postgresql://host:5432/dbname"
    val tableName = "myschema.mytable"
    val result = JdbcTools.getCanonicalTableName(tableName, jdbcURL)
    assert(result == "myschema.mytable")
  }

  test("getCanonicalTableName should handle quoted table names") {
    val jdbcURL = "jdbc:postgresql://host:5432/dbname"
    val tableName = "`mytable`"
    val result = JdbcTools.getCanonicalTableName(tableName, jdbcURL)
    assert(result == "`dbname.mytable`")
  }

  test("getCanonicalTableName should handle quoted schema.table") {
    val jdbcURL = "jdbc:postgresql://host:5432/dbname"
    val tableName = "\"myschema.mytable\""
    val result = JdbcTools.getCanonicalTableName(tableName, jdbcURL)
    assert(result == "`myschema.mytable`")
  }

}
