package com.databricks.labs.mosaic.datasource

import com.databricks.labs.mosaic.datasource.OGRFileFormat.{buildReaderImpl, inferSchemaImpl}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}

class ShapefileFileFormat extends OGRFileFormat with DataSourceRegister {

    override def shortName(): String = "shapefile"

    private val driverName = "ESRI Shapefile"

    override def inferSchema(
        sparkSession: SparkSession,
        options: Map[String, String],
        files: Seq[FileStatus]
    ): Option[StructType] = {
        val layerN = options.getOrElse("layerNumber", "0").toInt
        val inferenceLimit = options.getOrElse("inferenceLimit", "200").toInt
        val useZipPath = options.getOrElse("vsizip", "false").toBoolean
        inferSchemaImpl(driverName, layerN, inferenceLimit, useZipPath, files)
    }

    override def buildReader(
        sparkSession: SparkSession,
        dataSchema: StructType,
        partitionSchema: StructType,
        requiredSchema: StructType,
        filters: Seq[Filter],
        options: Map[String, String],
        hadoopConf: Configuration
    ): PartitionedFile => Iterator[InternalRow] = {
        val layerN = options.getOrElse("layerNumber", "0").toInt
        val useZipPath = options.getOrElse("vsizip", "false").toBoolean
        buildReaderImpl(driverName, layerN, useZipPath, dataSchema)
    }

}
