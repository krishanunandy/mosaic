package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVectorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
  * SQL expression that returns the last point of a linestring as a point.
  * @param inputGeom
  *   Expression containing the geometry.
  * @param expressionConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_Endpoint(
    inputGeom: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVectorExpression[ST_Endpoint](inputGeom, returnsGeometry = true, expressionConfig) {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry): Any = geometry.getEndpoint

    override def geometryCodeGen(geometryRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""$mosaicGeomClass = $geometryRef.getEndpoint();"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Endpoint extends WithExpressionInfo {

    override def name: String = "st_endpoint"

    override def usage: String = "_FUNC_(expr1) - Returns the last point of a linestring as a point."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        POINT(1.1, 2.2)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Endpoint](1, expressionConfig)
    }

}