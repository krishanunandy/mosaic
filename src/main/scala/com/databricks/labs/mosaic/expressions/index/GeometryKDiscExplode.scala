package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemID}
import com.databricks.labs.mosaic.core.types.{HexType, InternalGeometryType}
import com.databricks.labs.mosaic.core.Mosaic
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class GeometryKDiscExplode(geom: Expression, resolution: Expression, k: Expression, indexSystemName: String, geometryAPIName: String)
    extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    val indexSystem: IndexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
    val geometryAPI: GeometryAPI = GeometryAPI(geometryAPIName)

    override def position: Boolean = false

    override def inline: Boolean = false

    override def children: Seq[Expression] = Seq(geom, resolution, k)

    // noinspection DuplicatedCode
    override def checkInputDataTypes(): TypeCheckResult = {
        if (!Seq(BinaryType, StringType, HexType, InternalGeometryType).contains(geom.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported geom type.")
        } else if (!Seq(IntegerType, StringType).contains(resolution.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported resolution type.")
        } else if (!Seq(IntegerType).contains(k.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported k type.")
        } else {
            TypeCheckResult.TypeCheckSuccess
        }
    }

    //noinspection DuplicatedCode
    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        val geometryRaw = geom.eval(input)
        val resolutionRaw = resolution.eval(input)
        val kRaw = k.eval(input)
        if (geometryRaw == null || resolutionRaw == null || kRaw == null) {
            Seq.empty
        } else {
            val geometryVal = geometryAPI.geometry(geometryRaw, geom.dataType)
            val resolutionVal = indexSystem.getResolution(resolutionRaw)
            val kVal = kRaw.asInstanceOf[Int]
            val n = kVal - 1

            val chips = Mosaic.getChips(geometryVal, resolutionVal, keepCoreGeom = false, indexSystem, geometryAPI)
            val (coreChips, borderChips) = chips.partition(_.isCore)

            val coreIndices = coreChips.map(_.cellIdAsLong(indexSystem)).toSet
            val borderIndices = borderChips.map(_.cellIdAsLong(indexSystem))

            // We use nRing as naming for kRing where k = n
            val borderNRing = borderIndices.flatMap(indexSystem.kRing(_, n)).toSet
            val nRing = coreIndices ++ borderNRing

            val borderKDisc = borderIndices.flatMap(indexSystem.kDisc(_, kVal)).toSet

            val kDisc = borderKDisc -- nRing

            kDisc.map(row => InternalRow.fromSeq(Seq(indexSystem.serializeCellId(row))))
        }
    }

    override def elementSchema: StructType = StructType(Seq(StructField("cellId", indexSystem.getCellIdDataType)))

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
        copy(newChildren(0), newChildren(1), newChildren(2))

}

object GeometryKDiscExplode {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[GeometryKDiscExplode].getCanonicalName,
          db.orNull,
          "grid_cellkdiscexplode",
          """
            |    _FUNC_(cell_id, resolution)) - Generates the geometry based kdisc cell IDs set for the input
            |    geometry and the input k value.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        622236721274716159
            |        622236721274716160
            |        622236721274716161
            |        ...
            |
            |  """.stripMargin,
          "",
          "generator_funcs",
          "1.0",
          "",
          "built-in"
        )
}
