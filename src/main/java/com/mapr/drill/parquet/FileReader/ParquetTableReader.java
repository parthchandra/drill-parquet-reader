package com.mapr.drill.parquet.FileReader;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.DrillPathFilter;
import org.apache.drill.exec.store.parquet3.Metadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;

public class ParquetTableReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetTableReader.class);

  private final Configuration dfsConfig = new Configuration();
  private final FileSystem fs;
  private final List<ParquetMetadata> parquetMetadata = Lists.newArrayList();
  private final List<FileStatus> fileStatuses = Lists.newArrayList();
  private final List<RowGroupInfo> rowGroupInfos = Lists.newArrayList();

  private String pathName;

  ParquetTableReader(String pathName) throws IOException {
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
      ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(dfsConfig, fileStatus);
      this.parquetMetadata.add(parquetMetadata);
      List<BlockMetaData> blocks = parquetMetadata.getBlocks();
      int rowGroupIndex = 0;
      for (BlockMetaData block : blocks) {
        RowGroupInfo rowGroupInfo = new RowGroupInfo();
        rowGroupInfo.index = ++rowGroupIndex;
        rowGroupInfo.filePath = fileStatus.getPath().toString();
        rowGroupInfo.fileStatus = fileStatus;
        rowGroupInfo.rowGroup = block;
        List<ColumnInfo> columns = getColumns(block);
        rowGroupInfo.columns = columns;
        long length = 0;
        for(ColumnInfo col : columns){
          length += col.totalSize;
        }
        Map<String, Float> hostAffinity =
            getHostAffinity(fileStatus, block.getStartingPos(), length);
        rowGroupInfo.hostAffinity = hostAffinity;
        String hostname = InetAddress.getLocalHost().getHostName();
        Float localAffinity = hostAffinity.get(hostname);
        if(localAffinity == null){
          // useful only for testing. Otherwise bogus.
          localAffinity = hostAffinity.get("localhost");
        }
        rowGroupInfo.localAffinity = localAffinity != null ? localAffinity.floatValue() : 0;
        this.rowGroupInfos.add(rowGroupInfo);
        try {
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    return;
  }

  private List<ColumnInfo> getColumns(BlockMetaData block) {
    final List<ColumnChunkMetaData> parquetColumns = block.getColumns();
    final List<ColumnInfo> columns = Lists.newArrayList();
    for (ColumnChunkMetaData columnMetadata : parquetColumns) {
      ColumnInfo colInfo = new ColumnInfo();
      colInfo.columnName = columnMetadata.getPath().toDotString();
      colInfo.startPos = columnMetadata.getStartingPos();
      colInfo.totalSize = columnMetadata.getTotalSize();
      colInfo.columnMetadata = columnMetadata;
      columns.add(colInfo);
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
    ColumnChunkMetaData columnMetadata;
  }

  //TODO: Move this to an Executor class
  private static <T> List<T> runAllInParallel(int parallelism, List<Callable<T>> toRun) throws
      ExecutionException {
    ExecutorService threadPool = Executors.newFixedThreadPool(parallelism);

    try {
      ArrayList futures = new ArrayList();
      Iterator result = toRun.iterator();

      while(result.hasNext()) {
        Callable i$ = (Callable)result.next();
        futures.add(threadPool.submit(i$));
      }

      ArrayList result1 = new ArrayList(toRun.size());
      Iterator i$1 = futures.iterator();

      while(i$1.hasNext()) {
        Future future = (Future)i$1.next();

        try {
          result1.add(future.get());
        } catch (InterruptedException var11) {
          throw new RuntimeException("The thread was interrupted", var11);
        }
      }

      ArrayList i$2 = result1;
      return i$2;
    } finally {
      threadPool.shutdownNow();
    }
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("Usage: ParquetTableReader block|page filepath");
      return;
    }
    String whichOne = args[0];
    String fileName = args[1];

    ParquetTableReader reader = null;
    List<Callable<Object>> runnables = Lists.newArrayList();
    final DrillConfig config = DrillConfig.create();
    final BufferAllocator allocator = RootAllocatorFactory.newRoot(config);
    final Configuration dfsConfig = new Configuration();
    logger.info("Parquet Table Reader - Beginning new run to read {}.", fileName);
    try {
      //Ready
      reader = new ParquetTableReader(fileName);
      reader.getMetadata();
      //Set
      for (RowGroupInfo rg : reader.rowGroupInfos) {
        // Create a new Runnable for every column for every row group, if the row group
        // has local affinity of 1.0. Otherwise log the info that the row group was skipped.
        if (rg.localAffinity > 0.9) {
          for (ColumnInfo columnInfo : rg.columns) {
            RunnableReader runnable =
                new RunnableBlockReader(allocator, dfsConfig, rg.fileStatus, columnInfo);
            logger.info("[READING]\t{}\t{}\t{}\t{}\t{}", rg.filePath, "RowGroup-" + rg.index,
                columnInfo.columnName, columnInfo.startPos, columnInfo.totalSize);
            runnables.add(Executors.callable(runnable));
          }
        } else {
          logger.info("[SKIPPING]\t{}\t{}", rg.filePath, "RowGroup-" + rg.index);
        }
      }
      // Go
      reader.runAllInParallel(2, runnables);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    try {
      reader.init(whichOne);
      //reader.run();
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    return;
  }
}
