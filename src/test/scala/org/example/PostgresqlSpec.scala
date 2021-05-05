package org.example

import org.apache.log4j.{Level, Logger}
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.PostgreSQLContainer
import org.apache.spark.sql.functions.spark_partition_id
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.DriverManager
import java.util.Properties

class PostgresqlSpec extends AnyFlatSpec with TestContainerForAll {
  Logger.getLogger("org").setLevel(Level.OFF)

  override val containerDef = PostgreSQLContainer.Def("postgres")

  val testTableName = "users"
  val numPartitions = 10
  val partitionSize = 10

  "PostgreSQL data source" should "read table" in withContainers { postgresServer =>
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PostgresReaderJob")
      .getOrCreate()

    spark
      .read
      .format("org.example.datasource.postgres")
      .option("url", postgresServer.jdbcUrl)
      .option("user", postgresServer.username)
      .option("password", postgresServer.password)
      .option("tableName", testTableName)
      .option("numPartitions", numPartitions)
      .option("partitionSize", partitionSize)
      .load()
//      .withColumn("partition_id", spark_partition_id())
//      .orderBy("partition_id", "user_id")
      .withColumn("partition_id", spark_partition_id)
      .groupBy("partition_id").count
      .orderBy("partition_id")
      .show(100,false)

    spark.stop()
  }

  "PostgreSQL data source" should "write table" in withContainers { postgresServer =>
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PostgresWriterJob")
      .getOrCreate()

    import spark.implicits._

    val df = (60 to 70).map(_.toLong).toDF("user_id")

    df
      .write
      .format("org.example.datasource.postgres")
      .option("url", postgresServer.jdbcUrl)
      .option("user", postgresServer.username)
      .option("password", postgresServer.password)
      .option("tableName", testTableName)
      .mode(SaveMode.Append)
      .save()

    spark.stop()
  }

  override def startContainers(): Containers = {
    containerDef.start()
  }

  override def afterContainersStart(container: Containers): Unit = {
    super.afterContainersStart(container)

    container match {
      case c: PostgreSQLContainer => {
        val conn = connection(c)
        val stmt0 = conn.createStatement
        stmt0.execute(Queries.dropTableQuery)
        val stmt1 = conn.createStatement
        stmt1.execute(Queries.createTableQuery)
        val stmt2 = conn.createStatement
        stmt2.execute(Queries.insertDataQuery)
        conn.close()
      }
    }
  }
//  override def beforeContainersStop (container: Containers): Unit = {
//    super.beforeContainersStop(container)
//    container match {
//      case _: PostgreSQLContainer => {
//        Class.forName(container.driverClassName)
//
//        val conn = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)
////        conn.createStatement.execute(s"drop schema if exists $schema cascade;")
//        conn.close()
//      }
//    }
//  }

  def connection(c: PostgreSQLContainer) = {
    Class.forName(c.driverClassName)
    val properties = new Properties()
    properties.put("user", c.username)
    properties.put("password", c.password)
    DriverManager.getConnection(c.jdbcUrl, properties)
  }

  object Queries {
    lazy val dropTableQuery = s"DROP TABLE IF EXISTS $testTableName;"

    lazy val createTableQuery = s"CREATE TABLE $testTableName (user_id BIGINT PRIMARY KEY);"

    lazy val testValues: String = (1 to 50).map(i => s"($i)").mkString(", ")

    lazy val insertDataQuery = s"INSERT INTO $testTableName VALUES $testValues;"
  }

}
