package com.mapr.drill.parquet;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.proto.BitControl;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.TestOutputMutatorCopy;
import org.apache.drill.exec.store.TestParquetPhysicalOperator;
import org.apache.drill.exec.store.parquet.ParquetDirectByteBufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by pchandra on 12/15/15.
 */
public class ParquetReader implements Closeable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetReader.class);

  private static RemoteServiceSet serviceSet;
  private static DrillConfig config = DrillConfig.create();
  private static FunctionImplementationRegistry registry = new FunctionImplementationRegistry(config);
  private static ScanResult classpathScan = ClassPathScanner.fromPrescan(config);
  private BufferAllocator allocator = RootAllocatorFactory.newRoot(config);

  private Drillbit drillbit;
  private FragmentContext context;
  private final Configuration dfsConfig = new Configuration();
  //private final Mutator mutator = new Mutator();
  private FileSystem fs;

  private String which;
  private String filePath;
  private String columnName;
  private List<Footer> footers;
  private CodecFactory codecFactory;

  private void init(String which, String filePath, String columnName) throws Exception {
    allocator = RootAllocatorFactory.newRoot(config);
    drillbit = new Drillbit(config, serviceSet, classpathScan);
    drillbit.run();
    context =
        new FragmentContext(drillbit.getContext(), BitControl.PlanFragment.getDefaultInstance(), registry);
    fs = FileSystem.get(dfsConfig);
    this.which = which;
    this.filePath = filePath;
    this.columnName = columnName;
    footers = ParquetFileReader.readFooters(dfsConfig, new Path(this.filePath));
    codecFactory = CodecFactory
        .createDirectCodecFactory(dfsConfig, new ParquetDirectByteBufferAllocator(allocator), 0);
  }

  private List<SchemaPath> getColumns(BlockMetaData block) {
    final List<ColumnChunkMetaData> parquetColumns = block.getColumns();
    final List<SchemaPath> columns = Lists.newArrayList();
    if (this.columnName != null) {
      for (ColumnChunkMetaData columnMetadata : parquetColumns) {
        String columnName = columnMetadata.getPath().toDotString();
        if (this.columnName.equalsIgnoreCase(columnName)) {
          UserBitShared.NamePart namePart = UserBitShared.NamePart.newBuilder().setName(columnName).build();
          SchemaPath schemaPath = SchemaPath.create(namePart);
          columns.add(schemaPath);
          return columns;
        }
      }
    }
    for (ColumnChunkMetaData columnMetadata : parquetColumns) {
      String columnName = columnMetadata.getPath().toDotString();
      UserBitShared.NamePart namePart = UserBitShared.NamePart.newBuilder().setName(columnName).build();
      SchemaPath schemaPath = SchemaPath.create(namePart);
      columns.add(schemaPath);
    }
    return columns;
  }

  private void readAll() {
    Iterator iter = footers.iterator();
    while (iter.hasNext()) {
      Footer footer = (Footer) iter.next();
      int rowGroupIndex = 0;
      List<BlockMetaData> blocks = footer.getParquetMetadata().getBlocks();
      for (BlockMetaData block : blocks) {
        List<SchemaPath> columns = getColumns(block);
        try {
          read(footer, rowGroupIndex, columns);
        } catch (Exception e) {
          e.printStackTrace();
        }
        rowGroupIndex++;
      }
    }
  }

  private void read(Footer footer, int rowGroupIndex, List<SchemaPath> columns) throws Exception {
    final Path fileName = footer.getFile();
    int totalRowCount = 0;

    final AbstractRecordReader rr;

    if (this.which.equalsIgnoreCase("new")) {
      rr = new org.apache.drill.exec.store.parquet3.columnreaders.ParquetRecordReader(context,
          fileName.toString(), rowGroupIndex, fs, codecFactory, footer.getParquetMetadata(), columns);
    } else {
      rr = new org.apache.drill.exec.store.parquet.columnreaders.ParquetRecordReader(context,
          fileName.toString(), rowGroupIndex, fs, codecFactory, footer.getParquetMetadata(), columns);
    }

    OperatorContext oContext = context.newOperatorContext(new TestParquetPhysicalOperator("[Test]ParquetTableReader"));
    final TestOutputMutatorCopy mutator = new TestOutputMutatorCopy(oContext.getAllocator(), oContext);
    rr.setup(oContext, mutator);
    rr.allocate(mutator.getFieldVectorMap());
    final Stopwatch watch = new Stopwatch();
    watch.start();

    int rowCount = 0;
    while ((rowCount = rr.next()) > 0) {
      totalRowCount += rowCount;
    }
    long elapsed = watch.elapsed(TimeUnit.MICROSECONDS);
    System.out.println(String
        .format("Parquet Reader 1: %s, %d, %d, %d ", fileName.getName(), rowGroupIndex, totalRowCount,
            elapsed));
    rr.close();
    for (VectorWrapper<?> vvw : mutator.getContainer()) {
      vvw.clear();
    }
  }

  public void close() {
    allocator.close();
    drillbit.close();
  }


  public static void main(String[] args) {
    if (args.length != 2 && args.length != 3) {
      System.out.println("Usage: ParquetTableReader old|new filename [column_name]");
      return;
    }
    String whichOne = args[0];
    String fileName = args[1];
    String columnName = null;
    if(args.length == 3){
      columnName = args[2];
    }
    ParquetReader reader = new ParquetReader();
    try {
      reader.init(whichOne, fileName, columnName);
    } catch (Exception e) {
      e.printStackTrace();
    }
    reader.readAll();
    reader.close();
    return;
  }

}
