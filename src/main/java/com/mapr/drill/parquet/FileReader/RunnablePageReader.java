package com.mapr.drill.parquet.FileReader;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by pchandra on 5/5/16.
 * Reads (in one thread) an entire column, one page at a time.
 */
public class RunnablePageReader extends RunnableReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunnablePageReader.class);


  public RunnablePageReader(BufferAllocator allocator, Configuration dfsConfig, FileStatus fileStatus,
      ParquetTableReader.ColumnInfo columnInfo, int bufsize, boolean enableHints) throws IOException {
    super(allocator, dfsConfig, fileStatus, columnInfo, bufsize, enableHints);
  }

  @Override public void run() {
    String fileName = fileStatus.getPath().toString();
    Thread.currentThread().setName("[" + fileName + "]." + columnInfo.columnName);
    stopwatch.start();
    reader.init();
    while (!shutdown && true) {
      try {
        DrillBuf buf = reader.getNext(BUFSZ - 1);
        if (buf == null)
          break;
        buf.release();
      } catch (Exception e) {
        e.printStackTrace();
        break;
      }
    }
    elapsedTime = stopwatch.elapsed(TimeUnit.MICROSECONDS);
    logger.info("[COMPLETED]\t{}\t{}\t{}\t{}\t{}", fileName, columnInfo.columnName, columnInfo.totalSize,
        elapsedTime, (columnInfo.totalSize*1000000)/(elapsedTime*1024*1024));
    try {
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void shutdown(){
    this.shutdown=true;
  }


}
