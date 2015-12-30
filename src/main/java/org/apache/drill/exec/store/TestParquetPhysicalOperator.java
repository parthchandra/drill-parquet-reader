package org.apache.drill.exec.store;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.graph.GraphVisitor;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.beans.CoreOperatorType;
import org.apache.drill.exec.record.BatchSchema;

import java.util.Iterator;
import java.util.List;

/**
 * Created by pchandra on 12/28/15.
 */
public class TestParquetPhysicalOperator  extends AbstractGroupScan {

  public TestParquetPhysicalOperator(String userName) {
    super(userName);
  }

  @Override public boolean isExecutable() {
        return false;
      }

  @Override public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return null;
  }

  @Override public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints)
      throws PhysicalOperatorSetupException {

  }

  @Override public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    return null;
  }

  @Override public int getMaxParallelizationWidth() {
    return 0;
  }

  @Override public String getDigest() {
    return null;
  }

  @Override public int getOperatorType(){
    return UserBitShared.CoreOperatorType.PARQUET_ROW_GROUP_SCAN_VALUE;
  }

  @Override public long getInitialAllocation() {
    return initialAllocation;
  }

  @Override public long getMaxAllocation() {
    return maxAllocation;
  }

  @Override public int getOperatorId() {
    return super.getOperatorId();
  }

}
