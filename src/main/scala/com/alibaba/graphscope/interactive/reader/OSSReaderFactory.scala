package com.alibaba.graphscope.interactive.reader
import com.alibaba.graphscope.interactive.BulkLoadConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

class OSSReader extends  IReader {
  /**
   * Read a source table into a data frame.
   *
   * @param path path to the source, oss/odps/local are supported.
   * @return The data frame.
   */
  override def Read(): DataFrame = ???
}

class OSSReaderFactory(val loadingConfig: BulkLoadConfig) extends IReaderFactory {
  override def CreateVertexReader(labelName : String, path: String, spark : SparkSession): IReader = {
    return null
  }

  override def CreateEdgeReader(labelName: String, srcLabel: String, dstLabel: String, path : String, sparkSession: SparkSession): IReader = ???
}
