package com.mapr.drill.parquet.FileReader;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by pchandra on 5/5/16.
 * Consumes the data in the queue.
 */
public class RunnablePageConsumer implements Runnable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RunnablePageConsumer.class);

  private final DrillBuf EMPTY_BUF;
  private final Queue<DrillBuf> queue;
  private final BufferAllocator allocator;

  public RunnablePageConsumer(BufferAllocator allocator, Queue<DrillBuf> q) throws IOException {
    EMPTY_BUF = allocator.getEmpty();
    this.queue = q;
    this.allocator = allocator;
  }

  @Override public void run() {
    DrillBuf b = null;
    // return;

    try {
      while ((b = ((LinkedBlockingQueue<DrillBuf>)this.queue).take()) != EMPTY_BUF) {
        if(b != null){
          //Consume
          b.release();
        }

        try {
          // Sleep for or do work equivalent to decompressing a
          // 1 MiB gzipped page. GZip decompression is about 60 MB/sec
          // so 1 MB is about 16.6 ms

          // Sleep 10 ms. Approximates processing data 1 MiB data at
          // about 100 MiB/sec
          Thread.sleep(10);
        } catch (Exception e) {
          //spin around if interrupted
        }

        // Nothing in the queue. Spin around
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

}
