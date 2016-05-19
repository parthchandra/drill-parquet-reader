/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.exec.store.parquet3.columnreaders;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.store.parquet3.columnreaders.ParquetRecordReader;
import org.apache.drill.exec.vector.*;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;

public class ParquetFixedWidthDictionaryReaders {

  static class DictionaryIntReader extends
      org.apache.drill.exec.store.parquet3.columnreaders.FixedByteAlignedReader<IntVector> {
    DictionaryIntReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, IntVector v,
                                SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      if (usingDictionary) {
        for (int i = 0; i < recordsReadInThisIteration; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readInteger());
        }
      }
    }
  }

  static class DictionaryFixedBinaryReader extends
      org.apache.drill.exec.store.parquet3.columnreaders.FixedByteAlignedReader<VarBinaryVector> {
    DictionaryFixedBinaryReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                        ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarBinaryVector v,
                        SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);
      readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
      readLength = (int) Math.ceil(readLengthInBits / 8.0);

      if (usingDictionary) {
        VarBinaryVector.Mutator mutator =  valueVec.getMutator();
        Binary currDictValToWrite = null;
        for (int i = 0; i < recordsReadInThisIteration; i++){
          currDictValToWrite = pageReader.dictionaryValueReader.readBytes();
          mutator.setSafe(valuesReadInCurrentPass + i, currDictValToWrite.toByteBuffer(), 0,
              currDictValToWrite.length());
        }
        // Set the write Index. The next page that gets read might be a page that does not use dictionary encoding
        // and we will go into the else condition below. The readField method of the parent class requires the
        // writer index to be set correctly.
        int writerIndex = valueVec.getBuffer().writerIndex();
        valueVec.getBuffer().setIndex(0, writerIndex + (int)readLength);
      } else {
        super.readField(recordsToReadInThisPass);
      }

      // TODO - replace this with fixed binary type in drill
      // now we need to write the lengths of each value
      int byteLength = dataTypeLengthInBits / 8;
      for (int i = 0; i < recordsToReadInThisPass; i++) {
        valueVec.getMutator().setValueLengthSafe(valuesReadInCurrentPass + i, byteLength);
      }
    }
  }

  static class DictionaryDecimal9Reader extends
      org.apache.drill.exec.store.parquet3.columnreaders.FixedByteAlignedReader<Decimal9Vector> {
    DictionaryDecimal9Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                        ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Decimal9Vector v,
                        SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      if (usingDictionary) {
        for (int i = 0; i < recordsReadInThisIteration; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readInteger());
        }
      }
    }
  }

  static class DictionaryTimeReader extends
      org.apache.drill.exec.store.parquet3.columnreaders.FixedByteAlignedReader<TimeVector> {
    DictionaryTimeReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                        ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, TimeVector v,
                        SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      if (usingDictionary) {
        for (int i = 0; i < recordsReadInThisIteration; i++){
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readInteger());
        }
      }
    }
  }

  static class DictionaryBigIntReader extends
      org.apache.drill.exec.store.parquet3.columnreaders.FixedByteAlignedReader<BigIntVector> {
    DictionaryBigIntReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, BigIntVector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        try {
        valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readLong());
        } catch ( Exception ex) {
          throw ex;
        }
      }
    }
  }

  static class DictionaryDecimal18Reader extends
      org.apache.drill.exec.store.parquet3.columnreaders.FixedByteAlignedReader<Decimal18Vector> {
    DictionaryDecimal18Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                           ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Decimal18Vector v,
                           SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        try {
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readLong());
        } catch ( Exception ex) {
          throw ex;
        }
      }
    }
  }

  static class DictionaryTimeStampReader extends
      org.apache.drill.exec.store.parquet3.columnreaders.FixedByteAlignedReader<TimeStampVector> {
    DictionaryTimeStampReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                           ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, TimeStampVector v,
                           SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        try {
          valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readLong());
        } catch ( Exception ex) {
          throw ex;
        }
      }
    }
  }

  static class DictionaryFloat4Reader extends
      org.apache.drill.exec.store.parquet3.columnreaders.FixedByteAlignedReader<Float4Vector> {
    DictionaryFloat4Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Float4Vector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readFloat());
      }
    }
  }

  static class DictionaryFloat8Reader extends
      org.apache.drill.exec.store.parquet3.columnreaders.FixedByteAlignedReader<Float8Vector> {
    DictionaryFloat8Reader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Float8Vector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readDouble());
      }
    }
  }
}
