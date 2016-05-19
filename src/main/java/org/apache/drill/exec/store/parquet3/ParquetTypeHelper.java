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
package org.apache.drill.exec.store.parquet3;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;

import java.util.HashMap;
import java.util.Map;

public class ParquetTypeHelper {
  private static Map<MinorType,PrimitiveTypeName> typeMap;
  private static Map<DataMode,Repetition> modeMap;
  private static Map<MinorType,OriginalType> originalTypeMap;

  static {
    typeMap = new HashMap();

                    typeMap.put(MinorType.TINYINT, PrimitiveTypeName.INT32);
                    typeMap.put(MinorType.UINT1, PrimitiveTypeName.INT32);
                    typeMap.put(MinorType.UINT2, PrimitiveTypeName.INT32);
                    typeMap.put(MinorType.SMALLINT, PrimitiveTypeName.INT32);
                    typeMap.put(MinorType.INT, PrimitiveTypeName.INT32);
                    typeMap.put(MinorType.UINT4, PrimitiveTypeName.INT32);
                    typeMap.put(MinorType.FLOAT4, PrimitiveTypeName.FLOAT);
                    typeMap.put(MinorType.TIME, PrimitiveTypeName.INT32);
                    typeMap.put(MinorType.INTERVALYEAR, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
                    typeMap.put(MinorType.DECIMAL9, PrimitiveTypeName.INT32);
                    typeMap.put(MinorType.BIGINT, PrimitiveTypeName.INT64);
                    typeMap.put(MinorType.UINT8, PrimitiveTypeName.INT64);
                    typeMap.put(MinorType.FLOAT8, PrimitiveTypeName.DOUBLE);
                    typeMap.put(MinorType.DATE, PrimitiveTypeName.INT32);
                    typeMap.put(MinorType.TIMESTAMP, PrimitiveTypeName.INT64);
                    typeMap.put(MinorType.DECIMAL18, PrimitiveTypeName.INT64);
                    typeMap.put(MinorType.INTERVALDAY, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
                    typeMap.put(MinorType.INTERVAL, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
                    typeMap.put(MinorType.DECIMAL28DENSE, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
                    typeMap.put(MinorType.DECIMAL38DENSE, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
                    typeMap.put(MinorType.DECIMAL38SPARSE, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
                    typeMap.put(MinorType.DECIMAL28SPARSE, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
                    typeMap.put(MinorType.VARBINARY, PrimitiveTypeName.BINARY);
                    typeMap.put(MinorType.VARCHAR, PrimitiveTypeName.BINARY);
                    typeMap.put(MinorType.VAR16CHAR, PrimitiveTypeName.BINARY);
                    typeMap.put(MinorType.BIT, PrimitiveTypeName.BOOLEAN);

    modeMap = new HashMap();

    modeMap.put(DataMode.REQUIRED, Repetition.REQUIRED);
    modeMap.put(DataMode.OPTIONAL, Repetition.OPTIONAL);
    modeMap.put(DataMode.REPEATED, Repetition.REPEATED);

    originalTypeMap = new HashMap();

            originalTypeMap.put(MinorType.DECIMAL9,OriginalType.DECIMAL);
            originalTypeMap.put(MinorType.DECIMAL18,OriginalType.DECIMAL);
            originalTypeMap.put(MinorType.DECIMAL28DENSE,OriginalType.DECIMAL);
            originalTypeMap.put(MinorType.DECIMAL38DENSE,OriginalType.DECIMAL);
            originalTypeMap.put(MinorType.DECIMAL38SPARSE,OriginalType.DECIMAL);
            originalTypeMap.put(MinorType.DECIMAL28SPARSE,OriginalType.DECIMAL);
            originalTypeMap.put(MinorType.VARCHAR, OriginalType.UTF8);
            originalTypeMap.put(MinorType.DATE, OriginalType.DATE);
            originalTypeMap.put(MinorType.TIME, OriginalType.TIME_MILLIS);
            originalTypeMap.put(MinorType.TIMESTAMP, OriginalType.TIMESTAMP_MILLIS);
            originalTypeMap.put(MinorType.INTERVALDAY, OriginalType.INTERVAL);
            originalTypeMap.put(MinorType.INTERVALYEAR, OriginalType.INTERVAL);
            originalTypeMap.put(MinorType.INTERVAL, OriginalType.INTERVAL);
//            originalTypeMap.put(MinorType.TIMESTAMPTZ, OriginalType.TIMESTAMPTZ);
  }

  public static PrimitiveTypeName getPrimitiveTypeNameForMinorType(MinorType minorType) {
    return typeMap.get(minorType);
  }

  public static Repetition getRepetitionForDataMode(DataMode dataMode) {
    return modeMap.get(dataMode);
  }

  public static OriginalType getOriginalTypeForMinorType(MinorType minorType) {
    return originalTypeMap.get(minorType);
  }

  public static DecimalMetadata getDecimalMetadataForField(MaterializedField field) {
    switch(field.getType().getMinorType()) {
      case DECIMAL9:
      case DECIMAL18:
      case DECIMAL28SPARSE:
      case DECIMAL28DENSE:
      case DECIMAL38SPARSE:
      case DECIMAL38DENSE:
        return new DecimalMetadata(field.getPrecision(), field.getScale());
      default:
        return null;
    }
  }

  public static int getLengthForMinorType(MinorType minorType) {
    switch(minorType) {
      case INTERVALDAY:
      case INTERVALYEAR:
      case INTERVAL:
        return 12;
      case DECIMAL28SPARSE:
        return 12;
      case DECIMAL38SPARSE:
        return 16;
      default:
        return 0;
    }
  }

}
