package com.mapr.drill.parquet.FileReader;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import java.util.List;

/**
 * Created by pchandra on 5/11/16.
 */
public class TestBufferedReader {

  public static void main(String[] args) {
    final DrillConfig config = DrillConfig.create();
    final BufferAllocator allocator = RootAllocatorFactory.newRoot(config);
    final Configuration dfsConfig = new Configuration();
    String readerType = args[0];
    String fileName = args[1];
    Path filePath = new Path(fileName);
    Path outFilePath = new Path(fileName+".out.parquet");
    final int BUFSZ = 8*1024*1024;
    try {
      List<Footer> footers = ParquetFileReader.readFooters(dfsConfig, filePath);
      Footer footer = (Footer) footers.iterator().next();
      FileSystem fs = FileSystem.get(dfsConfig);
      int rowGroupIndex = 0;
      List<BlockMetaData> blocks = footer.getParquetMetadata().getBlocks();
      for (BlockMetaData block : blocks) {
        List<ColumnChunkMetaData> columns = block.getColumns();
        for (ColumnChunkMetaData columnMetadata : columns) {
          FSDataInputStream inputStream = fs.open(filePath);
          long startOffset = columnMetadata.getStartingPos();
          long totalByteSize = columnMetadata.getTotalSize();
          String streamId = fileName + ":" + columnMetadata.toString();
          BufferedDirectBufInputStream reader;
          if (readerType.equalsIgnoreCase("Chunked")) {
            reader = new ChunkedBufferedDirectBufInputStream(inputStream, allocator, streamId, startOffset,
                totalByteSize, BUFSZ);
          } else if (readerType.equalsIgnoreCase("Basic")) {
            reader = new BasicBufferedDirectBufInputStream(inputStream, allocator, streamId, startOffset,
                totalByteSize, BUFSZ);
          } else {
            return;
          }
          reader.init();
          while (true) {
            try {
              DrillBuf buf = reader.getNext(BUFSZ - 1);
              if (buf == null)
                break;
              {
                //TODO: write file


              }
              buf.release();
            } catch (Exception e) {
              e.printStackTrace();
              break;
            }
          }
          reader.close();
        }
      } // for each Block
    } catch (Exception e) {
      e.printStackTrace();
    }
    allocator.close();
    return;
  }
}
