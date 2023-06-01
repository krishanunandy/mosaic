package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_EndpointTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_EndpointBehaviors {

    testAllGeometriesNoCodegen("Testing stEndpoint") { endpointBehavior }
    testAllGeometriesCodegen("Testing stEndpoint CODEGEN compilation") { endpointBehavior }
    testAllGeometriesCodegen("Testing stEndpoint CODEGEN") { endpointBehavior }
    testAllGeometriesNoCodegen("Testing stEndpoint auxiliaryMethods") { auxiliaryMethods }

}