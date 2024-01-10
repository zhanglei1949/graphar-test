package com.alibaba.graphscope.interactive.reader

import com.alibaba.graphscope.interactive.{BulkLoadConfig, LoadingConfig, Schema}
import org.slf4j.LoggerFactory


object ReaderFactory {
  private val LOG = LoggerFactory.getLogger(this.getClass);

  def Create(loadingConfig: BulkLoadConfig, schema : Schema): IReaderFactory = {
    val scheme = loadingConfig.loading_config.data_source.scheme
    if (scheme.equals("odps") || scheme.equals("ODPS")) {
      LOG.info("Creating LoaderFactory for ODPS Table")
      return new ODPSReaderFactory(loadingConfig, schema);
    }
    else if (scheme.equals("file") || scheme.equals("FILE")) {
      LOG.info("Creating LoaderFactory from local file input")
      return new LocalFileReaderFactory(loadingConfig, schema)
    }
    else if (scheme.equals("oss") || scheme.equals("OSS")) {
      LOG.info("Creating LoaderFactory for oss input");
      return new OSSReaderFactory(loadingConfig)
    }
    else {
      LOG.error("Not recognized scheme: {}", scheme)
      throw new RuntimeException("Not recognized scheme: " + scheme)
    }
  }
}
