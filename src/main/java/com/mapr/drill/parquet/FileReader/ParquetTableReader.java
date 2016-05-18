package com.mapr.drill.parquet.FileReader;

import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.hadoop.fs.FileSystem;

/**
 * Created by pchandra on 5/5/16.
 */
public class ParquetTableReader {

  private DrillFileSystem fs;
  private String pathName;

  ParquetTableReader(DrillFileSystem fs, String pathName) {
    this.fs = fs;
    this.pathName = pathName;
  }

  /*
    Initialize thread pool.
    Read FileStatuses
    Read Footers
    Read RowGroups
    Read ColumnHeaders
    Setup ColumnReaderRunnables and queue them up
    Run
   */
  public void init(){
    return;
  }

}
