package com.mapr.drill.parquet.FileReader;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.util.filereader.BufferedDirectBufInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by pchandra on 5/5/16.
 * Reads (in one thread) an entire column, one block of data at a time.
 * Block size is 8 MB
 */
public class RunnableBlockReader extends RunnableReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunnableBlockReader.class);


  private boolean shutdown = false;

  public RunnableBlockReader(BufferAllocator allocator, Configuration dfsConfig, FileStatus fileStatus,
      ParquetTableReader.ColumnInfo columnInfo, int bufsize, boolean enableHints) throws IOException {
    super(allocator, dfsConfig, fileStatus, columnInfo, bufsize, enableHints);
  }

  @Override
  public RunnableReader.ReadStatus call() {
    RunnableReader.ReadStatus readStatus = new RunnableReader.ReadStatus();
    String fileName = fileStatus.getPath().toString();
    Thread.currentThread().setName("[" + fileName + "]." + columnInfo.columnName);
    long bytesRead = 0;
    readStatus.e = null;
    readStatus.bytesRead = bytesRead;
    readStatus.returnVal = 0;
    stopwatch.start();
    try (
        BufferedDirectBufInputStream reader =
            new BufferedDirectBufInputStream(inputStream, allocator, fileStatus.getPath().toString(),
                columnInfo.startPos, columnInfo.totalSize, BUFSZ, enableHints);

    ) {
      reader.init();
    long totalSize = columnInfo.totalSize;
    long maxBytes = BUFSZ -1;

    while (!shutdown && true) {
      try {
        int bytesToRead = maxBytes < totalSize - bytesRead ? (int)maxBytes : (int) (totalSize - bytesRead) ;
        DrillBuf buf = reader.getNext((int)bytesToRead);
        bytesRead += buf.writerIndex();
        if (buf == null  || bytesRead == totalSize)
          break;
        buf.release();
      } catch (Exception e) {
        e.printStackTrace();
        break;
      }
    }
    elapsedTime = stopwatch.elapsed(TimeUnit.MICROSECONDS);
    logger.info("[COMPLETED] (Block Reader)\t{}\t{}\t{}\t{}\t{}", fileName, columnInfo.columnName, columnInfo.totalSize,
        elapsedTime, (columnInfo.totalSize*1000000)/(elapsedTime*1024*1024));
    } catch (Exception e) {
      readStatus.e = e;
      readStatus.returnVal = -1;
      readStatus.bytesRead = bytesRead;
    }
    return readStatus;
  }

  public void shutdown(){
    this.shutdown=true;
  }


}
