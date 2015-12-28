package org.apache.drill.exec.store.parquet3;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.CompatibilityUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by pchandra on 12/22/15.
 * <p/>
 * Reads a ColumnChunk from a Parquet file given the columnChunkMetaData.
 * Data is read from disk in chunks of configurable size and reading is asynchronous.
 * No more than two chunks are kept in memory at a time.
 */
public class ParquetColumnChunkReader  extends InputStream implements Closeable{

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ParquetColumnChunkReader.class);

  private final FSDataInputStream fileInputStream;
  private final BufferAllocator allocator;
  private final String fileName;
  private final ColumnChunkMetaData columnChunkMetadata;
  private final long startOffset;
  private final long totalByteSize;
  private final int chunkSize;

  // read offset in current column chunk
  private long currentOffset = 0;
  // read offset in the current chunk
  private int currentChunkOffset = 0;
  // number of chunks read
  private int numChunksRead = 0;
  // bytes remaining to be read in the current chunk
  private long bytesRemaining = 0;

  private DrillBuf currentChunk;

  public ParquetColumnChunkReader(FSDataInputStream fileInputStream, BufferAllocator allocator,
      String fileName, ColumnChunkMetaData columnChunkMetadata, int chunkSize) {
    this.fileInputStream = fileInputStream;
    this.allocator = allocator;
    this.fileName = fileName;
    this.columnChunkMetadata = columnChunkMetadata;
    this.startOffset = columnChunkMetadata.getStartingPos();
    this.totalByteSize = columnChunkMetadata.getTotalSize();
    this.chunkSize = chunkSize;
    this.currentChunk = null;
  }

  public void init() {
    try {
      fileInputStream.seek(startOffset);
    } catch (IOException e) {
      //TODO: Throw UserException here
    }
    readChunk();
  }


  /*
  * Gets the next bytes bytes starting at offset offset in the column chunk
  */
  private DrillBuf getNext(long offset, int bytes) {
    if (offset < currentOffset) {
      //TODO: Throw internal exception. Cannot be reading backwards
    }

    if(offset == totalByteSize){
      return null;
    }

    int offsetToRead = (int) ( offset - (numChunksRead == 0 ? 0 : (numChunksRead - 1) * (chunkSize)) );
    int bytesToRead = offset + bytes > totalByteSize ? (int) (totalByteSize - offset) : bytes;

    while ((bytesToRead) > bytesRemaining) {
      readChunk(); // bytesRemaining increases by the number of bytes read
      offsetToRead = 0;
    }

    DrillBuf newBuf = currentChunk.slice(offsetToRead, bytesToRead);
    currentChunkOffset = offsetToRead + bytesToRead;
    currentOffset = offset + bytesToRead;
    bytesRemaining -= bytesToRead;
    logger.trace("Read {} bytes at current chunk offset {}. There are {} bytes remaining.", bytesToRead,
        currentChunkOffset, bytesRemaining);
    logger.trace("Current offset is {}. Number of Chunks read is {}", currentOffset, numChunksRead);
    return newBuf;
  }

  /*
   *  Get the next bytes bytes
   */
  public DrillBuf getNext(int bytes) {
    return getNext(currentOffset, bytes);
  }

  /*
  * Returns a DrillBuf corresponding to the raw bytes of the next page.
  *
  */
  public DrillBuf getNextPage() throws IOException {
    //TODO: read the page header and then read the page. Do not decode or decompress
    PageHeader pageHeader = Util.readPageHeader(this);
    pageHeader.getCompressed_page_size();




    return null;
  }

  /**
   * Implements the read method for InputStream. This is used in the Parquet method to
   * read the Page Header.
   *
   * @return
   * @throws IOException
   */
  @Override
  public int read() throws IOException {
    DrillBuf buf = getNext(1);
    if (buf == null || buf.nioBuffer().remaining() <= 0 ) {
      return -1;
    }
    return buf.nioBuffer().get() & 0xFF;
  }


  @Override
  public int read(byte[] bytes, int off, int len)
      throws IOException {
    DrillBuf buf = getNext(off, len);
    if (buf == null || buf.nioBuffer().remaining() <= 0 ) {
      return -1;
    }

    len = Math.min(len, buf.nioBuffer().remaining());
    buf.nioBuffer().get(bytes, off, len);
    return len;
  }


  /*
     * Skips reading the next bytes bytes. Moves current read pointer forward
     * by bytes bytes in the file. Releases the currentChunk and reads a new chunk.
     *
     */
  public long skip(long bytes) throws IOException {
    long bytesToSkip = bytes;
    if (bytes > (totalByteSize - currentOffset)) {
      bytesToSkip = totalByteSize - currentOffset;
    }
    fileInputStream.seek(currentOffset + bytesToSkip);
    if (currentChunk != null) {
      currentChunk.release();
      currentChunk = null;
      currentOffset+=bytesToSkip;
      numChunksRead=(int)currentOffset/chunkSize;
    }
    return bytesToSkip;
  }

  /*
   * Reads the next chunk.
   */
  private DrillBuf readChunk() {
    try {
      int bytesToRead = chunkSize <= (totalByteSize - currentOffset - bytesRemaining) ?
          chunkSize :
          (int) (totalByteSize - currentOffset - bytesRemaining);
      DrillBuf chunk = allocator.buffer(bytesToRead + (int) bytesRemaining);
      chunk.clear();
      if (bytesRemaining > 0) {
        chunk.setBytes(0, currentChunk, currentChunkOffset, (int) bytesRemaining);
        chunk.writerIndex((int) bytesRemaining);
      }
      ByteBuffer directBuffer = chunk.nioBuffer((int)bytesRemaining, bytesToRead);
      int lengthLeftToRead = bytesToRead;
      while (lengthLeftToRead > 0) {
        lengthLeftToRead -= CompatibilityUtil.getBuf(fileInputStream, directBuffer, lengthLeftToRead);
      }
      chunk.writerIndex((int)bytesRemaining + bytesToRead);

      numChunksRead++;
      bytesRemaining += bytesToRead;

      if(currentChunk != null){
        currentChunk.release();
      }
      currentChunk = chunk;
      currentChunkOffset = 0;
      //TODO: set state variables correctly
      logger.trace("Column {}, Read Chunk # {}, {} bytes", columnChunkMetadata.toString(), numChunksRead,
          bytesToRead);
    } catch (IOException e) {
      //TODO: throw UserException here
    }
    return currentChunk;
  }

  public void close() throws IOException {
    currentChunk.release();
    fileInputStream.close();
    return;
  }

  public static void main(String[] args) {
    final DrillConfig config = DrillConfig.create();
    final BufferAllocator allocator = RootAllocatorFactory.newRoot(config);
    final Configuration dfsConfig = new Configuration();
    String fileName = args[0];
    Path filePath = new Path(fileName);
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
          ParquetColumnChunkReader reader =
              new ParquetColumnChunkReader(inputStream, allocator, fileName, columnMetadata,
                  8 * 1024 * 1024);
          reader.init();
          while (true) {
            try {
              DrillBuf buf = reader.getNext(8 * 1024 * 1024 - 1);
              if (buf == null)
                break;
            }catch (Exception e){
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
