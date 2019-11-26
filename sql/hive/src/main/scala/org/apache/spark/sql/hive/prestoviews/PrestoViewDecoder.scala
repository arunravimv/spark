
package org.apache.spark.sql.hive.prestoviews

import java.nio.charset.StandardCharsets

import com.google.common.base.Preconditions
import com.google.gson.Gson
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.spark.sql.types.StructType

case class DecodedPrestoViewObject(originalSql: String,
                                   catalog: String,
                                   schema: String,
                                   columns: Array[ColumnDefinition])

case class ColumnDefinition(name: String, `type`: String)

object PrestoViewDecodeParser {

  import org.apache.spark.sql.hive.client.HiveClientImpl._
  private val dataTypeMap = Map("varchar"-> "String")
  private val GSON = new Gson()
  private val VIEW_PREFIX = "/* Presto View: "
  private val VIEW_PREFIX_MARKER = "/* Presto View */"
  private val VIEW_SUFFIX = " */"
  def isPrestoView(sqlText: String): Boolean = {
    val result = sqlText.startsWith(VIEW_PREFIX)
    result
  }

  def isViewMarker(sqlText: String): Boolean = {
    sqlText.startsWith(VIEW_PREFIX_MARKER)
  }

  def decode(sqlText: String): (String, StructType) = {
    val result = decodeViewData(sqlText)
    val prestoViewJson = GSON.fromJson(result, classOf[DecodedPrestoViewObject])
    val originalSQL = prestoViewJson.originalSql.replaceAll(s"${prestoViewJson.catalog}.", "")
    val cols = prestoViewJson.columns
      .map(c => new FieldSchema(c.name, prestoToSparkMapping(c.`type`), "")).map(fromHiveColumn)

    (originalSQL,
    StructType(cols))
  }

  import java.util.Base64

  def decodeViewData(data: String): String = {
    Preconditions.checkArgument(
      data.startsWith(VIEW_PREFIX),
      "View data missing prefix: %s",
      data
    )
    Preconditions.checkArgument(
      data.endsWith(VIEW_SUFFIX),
      "View data missing suffix: %s",
      data
    )

    var encodedSQL = data.substring(VIEW_PREFIX.length)
    encodedSQL = encodedSQL.substring(0, encodedSQL.length - VIEW_SUFFIX.length)
    new String(Base64.getDecoder.decode(encodedSQL), StandardCharsets.UTF_8)
  }

  def prestoToSparkMapping(datatype: String): String = {
    dataTypeMap.getOrElse(datatype, datatype)
  }
}
