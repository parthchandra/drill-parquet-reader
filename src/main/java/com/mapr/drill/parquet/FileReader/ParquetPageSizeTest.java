package com.mapr.drill.parquet.FileReader;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.store.dfs.DrillPathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;

public class ParquetPageSizeTest {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetPageSizeTest.class);

  private final Configuration dfsConfig = new Configuration();
  private final FileSystem fs;
  private final List<ParquetMetadata> parquetMetadata = Lists.newArrayList();
  private final List<FileStatus> fileStatuses = Lists.newArrayList();
  private final List<RowGroupInfo> rowGroupInfos = Lists.newArrayList();

  private String pathName;

  ParquetPageSizeTest(String pathName) throws IOException {
    fs = FileSystem.get(dfsConfig);
    this.pathName = pathName;
  }

  /*
    Initialize thread pool.
    Read FileStatuses -- Get Metadata
    Read Footers --
    Read RowGroups --
    Read ColumnHeaders --
    Setup ColumnReaderRunnables and queue them up
    Run
   */

  public void init(String whichOne){
    return;
  }

  private List<FileStatus> getFileStatuses(FileStatus fileStatus) throws IOException {
    if (fileStatus.isDirectory()) {
      for (FileStatus child : fs.listStatus(fileStatus.getPath(), new DrillPathFilter())) {
        //fileStatuses.addAll(getFileStatuses(child));
        getFileStatuses(child);
      }
    } else {
      fileStatuses.add(fileStatus);
    }
    return fileStatuses;
  }

  private void getMetadata() throws IOException {
    // get All file Statuses
    // read Footers
    // compute affinities
    final List<RowGroupInfo> rowGroupsInfo = Lists.newArrayList();
    getFileStatuses(fs.getFileStatus(new Path(this.pathName)));
    for(FileStatus fileStatus : fileStatuses){
      try {
        ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(dfsConfig, fileStatus, ParquetMetadataConverter.NO_FILTER);
        this.parquetMetadata.add(parquetMetadata);
        List<BlockMetaData> blocks = parquetMetadata.getBlocks();
        int rowGroupIndex = 0;
        for (BlockMetaData block : blocks) {
          RowGroupInfo rowGroupInfo = new RowGroupInfo();
          rowGroupInfo.index = ++rowGroupIndex;
          rowGroupInfo.filePath = fileStatus.getPath().toString();
          rowGroupInfo.fileStatus = fileStatus;
          rowGroupInfo.rowGroup = block;
          List<ColumnInfo> columns = getColumns(rowGroupInfo.filePath, block);
          rowGroupInfo.columns = columns;
          //long length = 0;
          //for (ColumnInfo col : columns) {
          //  length += col.totalSize;
          //}
          for (ColumnInfo col : columns) {
            logger.info("[READING],{}.{},{},{},{},{}", rowGroupInfo.filePath, "RowGroup-" + rowGroupInfo.index,
                col.columnName, col.startPos, col.totalSize, col.actualSize);
          }
          this.rowGroupInfos.add(rowGroupInfo);
        }
      } catch (Exception e) {
        logger.info("Skipping {}.", fileStatus.getPath());
      }
    }
    return;
  }

  private List<ColumnInfo> getColumns(String filename, BlockMetaData block) {
    final List<ColumnChunkMetaData> parquetColumns = block.getColumns();
    final List<ColumnInfo> columns = Lists.newArrayList();
    ColumnChunkMetaData[] parquetColumnMetadata = parquetColumns.toArray( new ColumnChunkMetaData[parquetColumns.size()]);
    //for (ColumnChunkMetaData columnMetadata : parquetColumns) {
    for(int i = 0 ; i < parquetColumns.size(); i++){
      ColumnChunkMetaData columnMetadata = parquetColumns.get(i);
      ColumnInfo colInfo = new ColumnInfo();
      colInfo.columnName = columnMetadata.getPath().toDotString();
      colInfo.startPos = columnMetadata.getStartingPos();
      colInfo.totalSize = columnMetadata.getTotalSize();
      colInfo.columnMetadata = columnMetadata;
      // Calculate actual size based on offset of start and end
      long nextStartPos = -1;
      long actualSize = -1;
      // if last Column
      if(i == parquetColumns.size() - 1){
        long blockSize = block.getTotalByteSize();
        if(blockSize > 0) {
          nextStartPos = block.getTotalByteSize() + block.getStartingPos();
          actualSize = nextStartPos - columnMetadata.getStartingPos();
        } else {
          actualSize = columnMetadata.getTotalSize();
        }
      } else{
        ColumnChunkMetaData nextColumnMetadata = parquetColumns.get(i+1);
        nextStartPos = nextColumnMetadata.getStartingPos();
        actualSize = nextStartPos - columnMetadata.getStartingPos();
      }
      colInfo.actualSize = actualSize;
      columns.add(colInfo);
      if(colInfo.totalSize != colInfo.actualSize){
        logger.info("[ASSERT],{},{},{},{},{},{}", filename, "RowGroup-" + block.getStartingPos(),
            colInfo.columnName, colInfo.startPos, colInfo.totalSize, colInfo.actualSize);
      }
    }
    return columns;
  }

  private Map<String, Float> getHostAffinity(FileStatus fileStatus, long start, long length)
      throws IOException {
    BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, start, length);
    Map<String, Float> hostAffinityMap = Maps.newHashMap();
    for (BlockLocation blockLocation : blockLocations) {
      for (String host : blockLocation.getHosts()) {
        Float currentAffinity = hostAffinityMap.get(host);
        float blockStart = blockLocation.getOffset();
        float blockEnd = blockStart + blockLocation.getLength();
        float rowGroupEnd = start + length;
        Float newAffinity = (blockLocation.getLength() - (blockStart < start ? start - blockStart : 0) -
            (blockEnd > rowGroupEnd ? blockEnd - rowGroupEnd : 0)) / length;
        if (currentAffinity != null) {
          hostAffinityMap.put(host, currentAffinity + newAffinity);
        } else {
          hostAffinityMap.put(host, newAffinity);
        }
      }
    }
    return hostAffinityMap;
  }

  private class RowGroupInfo {
    int index;
    String filePath;
    FileStatus fileStatus;
    float localAffinity;
    BlockMetaData rowGroup;
    List<ColumnInfo> columns;
    Map<String, Float> hostAffinity;
  }


  public  class ColumnInfo {
    String columnName;
    long startPos;
    long totalSize;
    long actualSize;
    ColumnChunkMetaData columnMetadata;
  }

  public static void main(String[] args) {
    String fileName = args[0];
    ParquetPageSizeTest reader = null;
    logger.info("Parquet Page Size Test Reader - Beginning new run to read {}.", fileName);
    try {
      reader = new ParquetPageSizeTest(fileName);
      reader.getMetadata();
      /*
      for (RowGroupInfo rg : reader.rowGroupInfos) {
          for (ColumnInfo columnInfo : rg.columns) {
            logger.info("[READING],{}.{},{},{},{},{}", rg.filePath, "RowGroup-" + rg.index,
                columnInfo.columnName, columnInfo.startPos, columnInfo.totalSize, columnInfo.actualSize);
          }
      }
      */
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
    return;
  }
}
