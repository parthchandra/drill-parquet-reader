package com.mapr.drill.parquet.FileReader;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

/**
 * Created by pchandra on 5/5/16.
 * Reads (in one thread) an entire column, one page at a time.
 */
public class RunnablePageReader extends RunnableReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunnablePageReader.class);


  private static final int blockSize = 8*1024*1024; // * MiB


  public RunnablePageReader(BufferAllocator allocator, Configuration dfsConfig, FileStatus fileStatus,
      ParquetTableReader.ColumnInfo columnInfo) throws IOException {
    super(allocator, dfsConfig, fileStatus, columnInfo);
  }

  @Override public void run() {
    // TODO: Measure time to run, bytes read.
    String fileName = fileStatus.getPath().toString();
    Thread.currentThread().setName("[" + fileName + "]." + columnInfo.columnName);
    /*
    final Workload<BaseWorkloadConfig> workload = runner.getWorkload();
    try {
      while (workload.hasNext()) {
        if(shutdown || Thread.currentThread().isInterrupted()) {
          logger.info("Workload run interrupted by user.");
          break;
        }
        final WorkloadIteration iteration = workload.next();
        runner.run(iteration);
      }
      final WorkloadRunnerConfig config = runner.getConfig();
      config.getNotifier().onWorkloadComplete(runner, workload.getConfig(), config.getCollector().getWorkloadStats());
      runner.close();
    } catch (Exception e) {
      logger.info("Workload run interrupted by exception.");
    } finally {
    }
    */
  }

  public void shutdown(){
    this.shutdown=true;
  }


}
