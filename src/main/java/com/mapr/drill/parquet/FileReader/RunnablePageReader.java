package com.mapr.drill.parquet.FileReader;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.util.filereader.BufferedDirectBufInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by pchandra on 5/5/16.
 * Reads (in one thread) an entire column, one page at a time and puts in an output queue.
 */
public class RunnablePageReader extends RunnableReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunnablePageReader.class);

  private final DrillBuf EMPTY_BUF;
  private Queue queue;

  public RunnablePageReader(BufferAllocator allocator, Configuration dfsConfig, FileStatus fileStatus,
      ParquetTableReader.ColumnInfo columnInfo, int bufsize, boolean enableHints, Queue q) throws IOException {
    super(allocator, dfsConfig, fileStatus, columnInfo, bufsize, enableHints);
    EMPTY_BUF = allocator.getEmpty();
    this.queue = q;
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
        long totalValueCount = columnInfo.columnMetadata.getValueCount();
        long valuesRead = 0;
        while (!shutdown && true) {
          long pageHeaderStart = reader.getPos(); // Use a stupid way to find how many bytes the page header was
          PageHeader pageHeader = Util.readPageHeader(reader);

          long pageHeaderSize = reader.getPos() - pageHeaderStart;
          bytesRead += pageHeaderSize;
          int compressedSize = pageHeader.getCompressed_page_size();

          DrillBuf buf = reader.getNext(compressedSize);
          bytesRead += compressedSize;

          // TODO: Check value count instead of bytes Read

          //pageHeader.data_page_header.getNum_values();
          valuesRead += pageHeader.getType() == PageType.DICTIONARY_PAGE ?
              pageHeader.getDictionary_page_header().getNum_values() :
              pageHeader.getData_page_header().getNum_values();

          if (valuesRead >= totalValueCount) {
            ((LinkedBlockingQueue) queue).put(EMPTY_BUF);
            break;
          }
          //if (buf == null  || bytesRead == totalSize){
          //  queue.add(EMPTY_BUF);
          //  break;
          //}
          ((LinkedBlockingQueue) queue).put(buf);
          try {
            ((LinkedBlockingQueue) queue).put(EMPTY_BUF);
          } catch (InterruptedException e1) {
            e1.printStackTrace();
          }
          break;
        } // while
        elapsedTime = stopwatch.elapsed(TimeUnit.MICROSECONDS);
        logger
            .info("[COMPLETED]\t{}\t{}\t{}\t{}\t{}", fileName, columnInfo.columnName, columnInfo.totalSize,
                elapsedTime, (columnInfo.totalSize * 1000000) / (elapsedTime * 1024 * 1024));
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
