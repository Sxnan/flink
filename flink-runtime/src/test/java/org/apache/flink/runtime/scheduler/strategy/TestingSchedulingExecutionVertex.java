/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.IterableUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A simple scheduling execution vertex for testing purposes. */
public class TestingSchedulingExecutionVertex implements SchedulingExecutionVertex {

    private final ExecutionVertexID executionVertexId;

    private final List<ConsumedPartitionGroup> consumedPartitionGroups;

    private final Collection<TestingSchedulingResultPartition> producedPartitions;

    private final Map<IntermediateResultPartitionID, TestingSchedulingResultPartition>
            resultPartitionsById;
    private final IntermediateDataSetID cachedIntermediateDataSetID;

    private ExecutionState executionState;

    public TestingSchedulingExecutionVertex(
            JobVertexID jobVertexId,
            int subtaskIndex,
            List<ConsumedPartitionGroup> consumedPartitionGroups,
            Map<IntermediateResultPartitionID, TestingSchedulingResultPartition>
                    resultPartitionsById,
            ExecutionState executionState,
            IntermediateDataSetID cachedIntermediateDataSetID) {

        this.executionVertexId = new ExecutionVertexID(jobVertexId, subtaskIndex);
        this.consumedPartitionGroups = checkNotNull(consumedPartitionGroups);
        this.producedPartitions = new ArrayList<>();
        this.resultPartitionsById = checkNotNull(resultPartitionsById);
        this.executionState = executionState;
        this.cachedIntermediateDataSetID = cachedIntermediateDataSetID;
    }

    @Override
    public ExecutionVertexID getId() {
        return executionVertexId;
    }

    @Override
    public ExecutionState getState() {
        return executionState;
    }

    public void setState(ExecutionState state) {
        this.executionState = state;
    }

    @Override
    public Iterable<TestingSchedulingResultPartition> getConsumedResults() {
        return IterableUtils.flatMap(consumedPartitionGroups, resultPartitionsById::get);
    }

    @Override
    public Iterable<TestingSchedulingResultPartition> getProducedResults() {
        return producedPartitions;
    }

    @Override
    public List<ConsumedPartitionGroup> getConsumedPartitionGroups() {
        return consumedPartitionGroups;
    }

    @Override
    public IntermediateDataSetID getCacheIntermediateDataSetID() {
        return cachedIntermediateDataSetID;
    }

    void addConsumedPartition(TestingSchedulingResultPartition consumedPartition) {
        final ConsumedPartitionGroup consumedPartitionGroup =
                ConsumedPartitionGroup.fromSinglePartition(consumedPartition.getId());

        consumedPartition.registerConsumedPartitionGroup(consumedPartitionGroup);
        if (consumedPartition.getState() == ResultPartitionState.CONSUMABLE) {
            consumedPartitionGroup.partitionFinished();
        }

        this.consumedPartitionGroups.add(consumedPartitionGroup);
        this.resultPartitionsById.putIfAbsent(consumedPartition.getId(), consumedPartition);
    }

    void addConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup,
            Map<IntermediateResultPartitionID, TestingSchedulingResultPartition>
                    consumedResultPartitionById) {
        this.consumedPartitionGroups.add(consumedPartitionGroup);
        this.resultPartitionsById.putAll(consumedResultPartitionById);
    }

    void addProducedPartition(TestingSchedulingResultPartition partition) {
        producedPartitions.add(partition);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static TestingSchedulingExecutionVertex withExecutionVertexID(
            JobVertexID jobVertexId, int subtaskIndex) {
        return newBuilder().withExecutionVertexID(jobVertexId, subtaskIndex).build();
    }

    public static TestingSchedulingExecutionVertex withCachedIntermediateDataset(
            IntermediateDataSetID cachedIntermediateDatasetID) {
        return newBuilder().withCachedIntermediateDataSetID(cachedIntermediateDatasetID).build();
    }

    /** Builder for {@link TestingSchedulingExecutionVertex}. */
    public static class Builder {
        private JobVertexID jobVertexId = new JobVertexID();
        private int subtaskIndex = 0;
        private final List<ConsumedPartitionGroup> consumedPartitionGroups = new ArrayList<>();
        private final Map<IntermediateResultPartitionID, TestingSchedulingResultPartition>
                resultPartitionsById = new HashMap<>();
        private ExecutionState executionState = ExecutionState.CREATED;
        private IntermediateDataSetID cachedIntermediateDataset = null;

        Builder withExecutionVertexID(JobVertexID jobVertexId, int subtaskIndex) {
            this.jobVertexId = jobVertexId;
            this.subtaskIndex = subtaskIndex;
            return this;
        }

        public Builder withConsumedPartitionGroups(
                List<ConsumedPartitionGroup> consumedPartitionGroups,
                Map<IntermediateResultPartitionID, TestingSchedulingResultPartition>
                        resultPartitionsById) {
            this.resultPartitionsById.putAll(resultPartitionsById);

            for (ConsumedPartitionGroup partitionGroup : consumedPartitionGroups) {
                List<IntermediateResultPartitionID> partitionIds =
                        new ArrayList<>(partitionGroup.size());
                for (IntermediateResultPartitionID partitionId : partitionGroup) {
                    partitionIds.add(partitionId);
                }
                this.consumedPartitionGroups.add(
                        ConsumedPartitionGroup.fromMultiplePartitions(partitionIds));
            }
            return this;
        }

        public Builder withExecutionState(ExecutionState executionState) {
            this.executionState = executionState;
            return this;
        }

        public TestingSchedulingExecutionVertex build() {
            return new TestingSchedulingExecutionVertex(
                    jobVertexId,
                    subtaskIndex,
                    consumedPartitionGroups,
                    resultPartitionsById,
                    executionState,
                    cachedIntermediateDataset);
        }

        public Builder withCachedIntermediateDataSetID(
                IntermediateDataSetID cachedIntermediateDatasetID) {
            this.cachedIntermediateDataset = cachedIntermediateDatasetID;
            return this;
        }
    }
}
