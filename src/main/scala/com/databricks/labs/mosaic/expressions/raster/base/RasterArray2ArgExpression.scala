package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.reflect.ClassTag

/**
  * Base class for all raster expressions that take two arguments. It provides
  * the boilerplate code needed to create a function builder for a given
  * expression. It minimises amount of code needed to create a new expression.
  *
  * @param rastersExpr
  *   The rasters expression. It is an array column containing rasters as either
  *   paths or as content byte arrays.
  * @param outputType
  *   The output type of the result.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterArray2ArgExpression[T <: Expression: ClassTag](
    rastersExpr: Expression,
    arg1Expr: Expression,
    arg2Expr: Expression,
    outputType: DataType,
    returnsRaster: Boolean,
    expressionConfig: MosaicExpressionConfig
) extends TernaryExpression
      with NullIntolerant
      with Serializable
      with RasterExpressionSerialization {

    /** Output Data Type */
    override def dataType: DataType = if (returnsRaster) rastersExpr.dataType.asInstanceOf[ArrayType].elementType else outputType

    override def first: Expression = rastersExpr

    override def second: Expression = arg1Expr

    override def third: Expression = arg2Expr

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the rasters to the expression.
      * It abstracts spark serialization from the caller.
      * @param rasters
      *   The sequence of rasters to be used.
      * @param arg1
      *   The first argument to the expression.
      * @param arg2
      *   The second argument to the expression.
      * @return
      *   A result of the expression.
      */
    def rasterTransform(rasters: Seq[MosaicRasterTile], arg1: Any, arg2: Any): Any

    /**
      * Evaluation of the expression. It evaluates the raster path and the loads
      * the raster from the path. It handles the clean up of the raster before
      * returning the results.
      * @param input
      *   The InternalRow of the expression. It contains an array containing
      *   raster tiles. It may be used for other argument expressions so it is
      *   passed to rasterTransform.
      *
      * @return
      *   The result of the expression.
      */
    override def nullSafeEval(input: Any, arg1: Any, arg2: Any): Any = {
        GDAL.enable(expressionConfig)
        serialize(
          rasterTransform(RasterArrayUtils.getTiles(input, rastersExpr, expressionConfig), arg1, arg2),
          returnsRaster,
          dataType,
          expressionConfig
        )
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 3, expressionConfig)

    override def withNewChildrenInternal(
        newFirst: Expression,
        newSecond: Expression,
        newThird: Expression
    ): Expression = makeCopy(Array(newFirst, newSecond, newThird))

}
