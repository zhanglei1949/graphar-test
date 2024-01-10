package com.alibaba.graphscope.interactive.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

trait IReader {
  /**
   * Read a source table into a data frame.
   * @param path path to the source, oss/odps/local are supported.
   * @return The data frame.
   */
  def Read() : DataFrame;
}

trait IReaderFactory {
  def CreateVertexReader(labelName : String, path : String, sparkSession: SparkSession) : IReader;
  def CreateEdgeReader(labelName: String, srcLabel : String, dstLabel : String, path : String, sparkSession: SparkSession ) : IReader
}
