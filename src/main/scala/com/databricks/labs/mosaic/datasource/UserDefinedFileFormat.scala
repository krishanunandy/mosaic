package com.databricks.labs.mosaic.datasource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType

class UserDefinedFileFormat extends FileFormat with DataSourceRegister {

    override def inferSchema(
        sparkSession: SparkSession,
        options: Map[String, String],
        files: Seq[FileStatus]
    ): Option[StructType] = {
        val readerClass = options("readerClass")
        val reader = Class.forName(readerClass).newInstance().asInstanceOf[UserDefinedReader]
        reader.inferSchema(sparkSession, options, files)
    }

    override def prepareWrite(
        sparkSession: SparkSession,
        job: Job,
        options: Map[String, String],
        dataSchema: StructType
    ): OutputWriterFactory = throw new UnsupportedOperationException("Write is not supported for this data source")

    override def shortName(): String = "user_defined_reader"

    override def buildReader(
        sparkSession: SparkSession,
        dataSchema: StructType,
        partitionSchema: StructType,
        requiredSchema: StructType,
        filters: Seq[Filter],
        options: Map[String, String],
        hadoopConf: Configuration
    ): PartitionedFile => Iterator[InternalRow] = {
        val readerClass = options("readerClass")
        val reader = Class.forName(readerClass).newInstance().asInstanceOf[UserDefinedReader]
        reader.buildReader(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)
    }

}
