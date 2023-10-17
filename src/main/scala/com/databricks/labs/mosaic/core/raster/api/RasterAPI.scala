package com.databricks.labs.mosaic.core.raster.api

import com.databricks.labs.mosaic.core.raster._
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.io.{RasterCleaner, RasterReader}
import com.databricks.labs.mosaic.core.raster.operator.transform.RasterTransform
import com.databricks.labs.mosaic.utils.PathUtils
import com.databricks.labs.mosaic.core.raster.gdal_raster.{MosaicRasterGDAL, RasterCleaner, RasterReader, RasterTransform}
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
  * A base trait for all Raster API's.
  * @param reader
  *   The RasterReader to use for reading the raster.
  */
abstract class RasterAPI(reader: RasterReader) extends Serializable {

    def getExtension(driverShortName: String): String

    def readRaster(
        inputRaster: Any,
        parentPath: String,
        shortDriverName: String,
        inputDT: DataType
    ): MosaicRaster = {
        inputDT match {
            case StringType =>
                val path = inputRaster.asInstanceOf[UTF8String].toString
                reader.readRaster(path, parentPath)
            case BinaryType =>
                val bytes = inputRaster.asInstanceOf[Array[Byte]]
                val raster = reader.readRaster(bytes, parentPath, shortDriverName)
                // If the raster is coming as a byte array, we can't check for zip condition.
                // We first try to read the raster directly, if it fails, we read it as a zip.
                Option(raster).getOrElse(
                  reader.readRaster(bytes, parentPath, shortDriverName)
                )
        }
    }

    def writeRasters(generatedRasters: Seq[MosaicRaster], checkpointPath: String, rasterDT: DataType): Seq[Any] = {
        generatedRasters.map(raster =>
            if (Option(raster).isDefined) {
                rasterDT match {
                    case StringType =>
                        val extension = raster.getRaster.GetDriver().GetMetadataItem("DMD_EXTENSION")
                        val writePath = s"$checkpointPath/${raster.uuid}.$extension"
                        val outPath = raster.writeToPath(writePath)
                        RasterCleaner.dispose(raster)
                        UTF8String.fromString(outPath)
                    case BinaryType =>
                        val bytes = raster.writeToBytes()
                        RasterCleaner.dispose(raster)
                        bytes
                }
            } else {
                null
            }
        )
    }

    /**
      * This method should be called in every raster expression if the RasterAPI
      * requires enablement on worker nodes.
      */
    def enable(): Unit

    /** @return Returns the name of the raster API. */
    def name: String

    /**
      * Reads a raster from the given path. Assume not zipped file. If zipped,
      * use raster(path, vsizip = true)
      *
      * @param path
      *   The path to the raster. This path has to be a path to a single raster.
      *   Rasters with subdatasets are supported.
      * @return
      *   Returns a Raster object.
      */
    def raster(path: String, parentPath: String): MosaicRaster =
        reader.readRaster(path, parentPath)

    def raster(content: Array[Byte], parentPath: String, driverShortName: String): MosaicRaster =
        reader.readRaster(content, parentPath, driverShortName)

    /**
      * Reads a raster from the given path. It extracts the specified band from
      * the raster. If zip, use band(path, bandIndex, vsizip = true)
      *
      * @param path
      *   The path to the raster. This path has to be a path to a single raster.
      *   Rasters with subdatasets are supported.
      * @param bandIndex
      *   The index of the band to read from the raster.
      * @return
      *   Returns a Raster band object.
      */
    def band(path: String, bandIndex: Int, parentPath: String): MosaicRasterBand =
        reader.readBand(path, bandIndex, parentPath)

    /**
      * Converts raster x, y coordinates to lat, lon coordinates.
      * @param gt
      *   Geo transform of the raster.
      * @param x
      *   X coordinate of the raster.
      * @param y
      *   Y coordinate of the raster.
      * @return
      *   Returns a tuple of (lat, lon).
      */
    def toWorldCoord(gt: Seq[Double], x: Int, y: Int): (Double, Double) = {
        val (xGeo, yGeo) = RasterTransform.toWorldCoord(gt, x, y)
        (xGeo, yGeo)
    }

    /**
      * Converts lat, lon coordinates to raster x, y coordinates.
      * @param gt
      *   Geo transform of the raster.
      * @param x
      *   Latitude of the raster.
      * @param y
      *   Longitude of the raster.
      * @return
      *   Returns a tuple of (xPixel, yPixel).
      */
    def fromWorldCoord(gt: Seq[Double], x: Double, y: Double): (Int, Int) = {
        val (xPixel, yPixel) = RasterTransform.fromWorldCoord(gt, x, y)
        (xPixel, yPixel)
    }

}

/**
  * A companion object for the RasterAPI trait. It provides a factory method for
  * creating a RasterAPI object.
  */
object RasterAPI extends Serializable {

    /**
      * Creates a RasterAPI object.
      * @param name
      *   The name of the API to use. Currently only GDAL is supported.
      * @return
      *   Returns a RasterAPI object.
      */
    def apply(name: String): RasterAPI =
        name match {
            case "GDAL" => GDAL
        }

    /**
      * @param name
      *   The name of the API to use. Currently only GDAL is supported.
      * @return
      *   Returns a RasterReader object.
      */
    def getReader(name: String): RasterReader =
        name match {
            case "GDAL" => MosaicRasterGDAL
        }



}
