package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait RST_WorldToRasterCoordXBehaviors extends QueryTest {

    def worldToRasterCoordXBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersInMemory = spark.read
            .format("gdal")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/binary/netcdf-coral")

        val df = rastersInMemory
            .withColumn("result", rst_worldtorastercoordx($"tile", 0, 0))
            .select("result")

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_worldtorastercoordx(tile, 1, 1) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("result", rst_worldtorastercoordx($"tile", 0, 0))
            .withColumn("result", rst_worldtorastercoordx($"tile", lit(0), lit(0)))
            .select("result")

        val result = df.as[String].collect().head.length

        result should be > 0

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_worldtorastercoordx() from source
                                                     |""".stripMargin)

    }

}
