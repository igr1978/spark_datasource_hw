package org.example.datasource.postgres

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.{DriverManager}
import java.util
import scala.collection.JavaConverters._


class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = PostgresTable.schema

  override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): Table = new PostgresTable(properties.get("tableName"))
}

class PostgresTable(val name: String) extends SupportsRead with SupportsWrite {
  override def schema(): StructType = PostgresTable.schema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new PostgresScanBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new PostgresWriteBuilder(info.options)
}

object PostgresTable {
  val schema: StructType = new StructType().add("user_id", LongType)
}

case class ConnectionProperties(url: String, user: String, password: String, tableName: String)

/** Read */
class PostgresScanBuilder(val options: CaseInsensitiveStringMap) extends ScanBuilder {
  val partitionSize = options.getInt("partitionSize", 1)
  override def build(): Scan = new PostgresScan(partitionSize, ConnectionProperties(
    options.get("url"), options.get("user"), options.get("password"), options.get("tableName")))
}

class PostgresPartition(val start:Int, val end:Int) extends InputPartition

class PostgresScan(val partitionSize: Int, val connectionProperties: ConnectionProperties) extends Scan with Batch{
  require(partitionSize > 0, "Size of partitions must be positive")

  override def readSchema(): StructType = PostgresTable.schema

  override def toBatch: Batch = this

  def getNumberPartitions: Int = {
    val tableName = connectionProperties.tableName
    val connection = DriverManager.getConnection(
      connectionProperties.url, connectionProperties.user, connectionProperties.password
    )

    val statement = connection.createStatement
    val resultSet = statement.executeQuery(s"SELECT count(*) FROM $tableName;")
    resultSet.next()
    val rowsCount = resultSet.getLong(1)
    connection.close()

    val numPartitions = Math.max(Math.round(rowsCount / partitionSize), 1).toInt
    numPartitions
  }

  override def planInputPartitions(): Array[InputPartition] = {
    val numPartitions = getNumberPartitions
    require(numPartitions > 0, "Number of partitions must be positive")

    val partitionList = new java.util.ArrayList[InputPartition]
    for (i <- 0 to numPartitions) {
      val start = i * partitionSize
      val end = start + partitionSize

      partitionList.add(new PostgresPartition(start, end))
    }
    partitionList.asScala.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new PostgresPartitionReaderFactory(connectionProperties)
  }
}

class PostgresPartitionReaderFactory(connectionProperties: ConnectionProperties)
  extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new
      PostgresPartitionReader(partition.asInstanceOf[PostgresPartition], connectionProperties)
}

class PostgresPartitionReader(postgresPartition: PostgresPartition, connectionProperties: ConnectionProperties)
  extends PartitionReader[InternalRow] {
  private val connection = DriverManager.getConnection(
    connectionProperties.url, connectionProperties.user, connectionProperties.password
  )

  var start = postgresPartition.start
  var limit = postgresPartition.end - start

  private val statement = connection.createStatement()
  private val resultSet = statement.executeQuery(s"select * from ${connectionProperties.tableName} order by user_id limit ${limit} offset ${start}")

  override def next(): Boolean = resultSet.next()

  override def get(): InternalRow = InternalRow(resultSet.getLong(1))

  override def close(): Unit = connection.close()

}


/** Write */

class PostgresWriteBuilder(options: CaseInsensitiveStringMap) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new PostgresBatchWrite(ConnectionProperties(
    options.get("url"), options.get("user"), options.get("password"), options.get("tableName")
  ))
}

class PostgresBatchWrite(connectionProperties: ConnectionProperties) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory =
    new PostgresDataWriterFactory(connectionProperties)

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class PostgresDataWriterFactory(connectionProperties: ConnectionProperties) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId:Long): DataWriter[InternalRow] =
    new PostgresWriter(connectionProperties)
}

object WriteSucceeded extends WriterCommitMessage

class PostgresWriter(connectionProperties: ConnectionProperties) extends DataWriter[InternalRow] {

  val connection = DriverManager.getConnection(
    connectionProperties.url,
    connectionProperties.user,
    connectionProperties.password
  )
  val statement = "insert into users (user_id) values (?)"
  val preparedStatement = connection.prepareStatement(statement)

  override def write(record: InternalRow): Unit = {
    val value = record.getLong(0)
    preparedStatement.setLong(1, value)
    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = connection.close()
}

