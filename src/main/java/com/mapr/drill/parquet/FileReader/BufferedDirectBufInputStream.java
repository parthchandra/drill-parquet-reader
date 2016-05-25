package com.mapr.drill.parquet.FileReader;

import io.netty.buffer.DrillBuf;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by pchandra on 5/5/16.
 */
public abstract class BufferedDirectBufInputStream extends FilterInputStream {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BufferedDirectBufInputStream.class);

  BufferedDirectBufInputStream(InputStream in){
    super(in);
  }

  public abstract void init();

  public abstract DrillBuf getNext(int bytes) throws IOException;

  @Override public abstract int read(byte[] b) throws IOException;

  @Override public abstract int read(byte[] b, int off, int len) throws IOException;


  protected void fadviseIfAvailable(FSDataInputStream inputStream, long off, long n) {
    Method readAhead;
    final Class adviceType;

    try {
      adviceType = Class.forName("org.apache.hadoop.fs.FSDataInputStream$FadviseType");
    } catch (ClassNotFoundException e) {
      logger.info("Unable to call fadvise due to: {}", e.toString());
      readAhead = null;
      return;
    }
    try {
      Class<? extends FSDataInputStream> inputStreamClass = inputStream.getClass();
      readAhead = inputStreamClass.getMethod("adviseFile", new Class[] {adviceType, long.class, long.class});
    } catch (NoSuchMethodException e) {
      logger.info("Unable to call fadvise due to: {}", e.toString());
      readAhead = null;
      return;
    }
    if (readAhead != null) {
      Object[] adviceTypeValues = adviceType.getEnumConstants();
      for(int idx = 0; idx < adviceTypeValues.length; idx++) {
        if((adviceTypeValues[idx]).toString().contains("SEQUENTIAL")) {
          try {
            readAhead.invoke(inputStream, adviceTypeValues[idx], off, n);
          } catch (IllegalAccessException e) {
            logger.info("Unable to call fadvise due to: {}", e.toString());
          } catch (InvocationTargetException e) {
            logger.info("Unable to call fadvise due to: {}", e.toString());
          }
          break;
        }
      }
    }
    return;
  }


}
