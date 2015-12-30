/**
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
 */
package org.apache.drill.exec.store;

import io.netty.buffer.DrillBuf;

import java.util.Map;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Maps;

/*
public class ScanBatchOutputMutator implements OutputMutator, Iterable<VectorWrapper<?>> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanBatchOutputMutator.class);

  private final VectorContainer container = new VectorContainer();
  private final Map<MaterializedField, ValueVector> fieldVectorMap = Maps.newHashMap();
  private final BufferAllocator allocator;

  public ScanBatchOutputMutator(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public void removeField(MaterializedField field) throws SchemaChangeException {
    ValueVector vector = fieldVectorMap.remove(field);
    if (vector == null) {
      throw new SchemaChangeException("Failure attempting to remove an unknown field.");
    }
    container.remove(vector);
    vector.close();
  }

  public void addField(ValueVector vector) {
    container.add(vector);
    fieldVectorMap.put(vector.getField(), vector);
  }

  private void replace(ValueVector newVector, SchemaPath schemaPath) {
    List<ValueVector> vectors = Lists.newArrayList();
    for (VectorWrapper w : container) {
      ValueVector vector = w.getValueVector();
      if (vector.getField().getPath().equals(schemaPath)) {
        vectors.add(newVector);
      } else {
        vectors.add(w.getValueVector());
      }
      container.remove(vector);
    }
    container.addCollection(vectors);
  }

  public Iterator<VectorWrapper<?>> iterator() {
    return container.iterator();
  }

  public void clear() {

  }

  public boolean isNewSchema() {
    return false;
  }

  public void allocate(int recordCount) {
    for (final ValueVector v : fieldVectorMap.values()) {
      AllocationHelper.allocate(v, recordCount, 50, 10);
    }
  }

  public <T extends ValueVector> T addField(MaterializedField field, Class<T> clazz) throws SchemaChangeException {
    ValueVector v = TypeHelper.getNewVector(field, allocator);
    if (!clazz.isAssignableFrom(v.getClass())) {
      throw new SchemaChangeException(String.format("The class that was provided %s does not correspond to the expected vector type of %s.", clazz.getSimpleName(), v.getClass().getSimpleName()));
    }
    addField(v);
    return (T) v;
  }

  public DrillBuf getManagedBuffer() {
    return allocator.buffer(255);
  }

  public CallBack getCallBack() {
    return null;
  }

  public VectorContainer getContainer() {
    return container;
  }

}
*/
public class TestOutputMutatorCopy implements OutputMutator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOutputMutatorCopy.class);

  private final VectorContainer container = new VectorContainer();
  private final Map<MaterializedField.Key, ValueVector> fieldVectorMap = Maps.newHashMap();
  private final BufferAllocator allocator;
  private final OperatorContext oContext;
  private SchemaChangeCallBack callBack = new SchemaChangeCallBack();

  /** Whether schema has changed since last inquiry (via #isNewSchema}).  Is
   *  true before first inquiry. */
  private boolean schemaChanged = true;

  public TestOutputMutatorCopy(BufferAllocator allocator, OperatorContext oContext) {
    this.allocator = allocator;
    this.oContext = oContext;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends ValueVector> T addField(MaterializedField field,
      Class<T> clazz) throws SchemaChangeException {
    // Check if the field exists.
    ValueVector v = fieldVectorMap.get(field.key());
    if (v == null || v.getClass() != clazz) {
      // Field does not exist--add it to the map and the output container.
      v = TypeHelper.getNewVector(field, oContext.getAllocator(), callBack);
      if (!clazz.isAssignableFrom(v.getClass())) {
        throw new SchemaChangeException(
            String.format(
                "The class that was provided, %s, does not correspond to the "
                    + "expected vector type of %s.",
                clazz.getSimpleName(), v.getClass().getSimpleName()));
      }

      final ValueVector old = fieldVectorMap.put(field.key(), v);
      if (old != null) {
        old.clear();
        container.remove(old);
      }

      container.add(v);
      // Added new vectors to the container--mark that the schema has changed.
      schemaChanged = true;
    }

    return clazz.cast(v);
  }

  @Override
  public void allocate(int recordCount) {
    for (final ValueVector v : fieldVectorMap.values()) {
      AllocationHelper.allocate(v, recordCount, 50, 10);
    }
  }

  /**
   * Reports whether schema has changed (field was added or re-added) since
   * last call to {@link #isNewSchema}.  Returns true at first call.
   */
  @Override
  public boolean isNewSchema() {
    // Check if top-level schema or any of the deeper map schemas has changed.

    // Note:  Callback's getSchemaChangedAndReset() must get called in order
    // to reset it and avoid false reports of schema changes in future.  (Be
    // careful with short-circuit OR (||) operator.)

    final boolean deeperSchemaChanged = callBack.getSchemaChangedAndReset();
    if (schemaChanged || deeperSchemaChanged) {
      schemaChanged = false;
      return true;
    }
    return false;
  }

  @Override
  public DrillBuf getManagedBuffer() {
    return oContext.getManagedBuffer();
  }

  @Override
  public CallBack getCallBack() {
    return callBack;
  }

  public VectorContainer getContainer(){
    return container;
  }

  public
  Map<MaterializedField.Key, ValueVector> getFieldVectorMap(){
    return fieldVectorMap;
  }

}

