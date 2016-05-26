package com.mapr.drill.parquet.FileReader;

import com.google.common.base.Stopwatch;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * Created by pchandra on 5/5/16.
 * Reads (in one thread) an entire column, one block of data at a time.
 * Block size is 8 MB
 */
public abstract class RunnableReader implements Runnable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunnableReader.class);

  protected final int BUFSZ;

  protected boolean shutdown = false;
  protected final FileStatus fileStatus;
  protected final ParquetTableReader.ColumnInfo columnInfo;

  protected final Stopwatch stopwatch = Stopwatch.createUnstarted();
  protected long elapsedTime;

  protected final BufferAllocator allocator;
  protected final Configuration dfsConfig;
  protected final FileSystem fs;
  protected final FSDataInputStream inputStream;
  protected final boolean enableHints;

  protected final BufferedDirectBufInputStream reader;


  public RunnableReader(BufferAllocator allocator, Configuration dfsConfig, FileStatus fileStatus,
      ParquetTableReader.ColumnInfo columnInfo, int bufsize, boolean enableHints) throws IOException {
    this.allocator = allocator;
    this.dfsConfig = dfsConfig;
    this.fileStatus = fileStatus;
    this.columnInfo = columnInfo;
    this.fs = FileSystem.get(dfsConfig);
    this.inputStream = fs.open(fileStatus.getPath());
    this.BUFSZ = bufsize;
    this.enableHints = enableHints;
    this.reader =
        new BasicBufferedDirectBufInputStream(inputStream, allocator, fileStatus.getPath().toString(),
            columnInfo.startPos, columnInfo.totalSize, BUFSZ, enableHints);

  }

  public void shutdown(){
    this.shutdown=true;
  }


}
