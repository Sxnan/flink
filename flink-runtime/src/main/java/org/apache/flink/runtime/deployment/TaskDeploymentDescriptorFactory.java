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

package org.apache.flink.runtime.deployment;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.InternalExecutionGraphAccessor;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.scheduler.CacheCorruptedException;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;
import org.apache.flink.types.Either;
import org.apache.flink.util.CompressedSerializedValue;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Factory of {@link TaskDeploymentDescriptor} to deploy {@link
 * org.apache.flink.runtime.taskmanager.Task} from {@link Execution}.
 */
public class TaskDeploymentDescriptorFactory {
    private final ExecutionAttemptID executionId;
    private final MaybeOffloaded<JobInformation> serializedJobInformation;
    private final MaybeOffloaded<TaskInformation> taskInfo;
    private final JobID jobID;
    private final PartitionLocationConstraint partitionDeploymentConstraint;
    private final List<ConsumedPartitionGroup> consumedPartitionGroups;
    private final Function<IntermediateResultPartitionID, IntermediateResultPartition>
            resultPartitionRetriever;
    private final BlobWriter blobWriter;
    private final Map<IntermediateDataSetID, ShuffleDescriptor[]>
            consumedClusterPartitionShuffleDescriptors;

    private TaskDeploymentDescriptorFactory(
            ExecutionAttemptID executionId,
            MaybeOffloaded<JobInformation> serializedJobInformation,
            MaybeOffloaded<TaskInformation> taskInfo,
            JobID jobID,
            PartitionLocationConstraint partitionDeploymentConstraint,
            List<ConsumedPartitionGroup> consumedPartitionGroups,
            Function<IntermediateResultPartitionID, IntermediateResultPartition>
                    resultPartitionRetriever,
            BlobWriter blobWriter,
            Map<IntermediateDataSetID, ShuffleDescriptor[]>
                    consumedClusterPartitionShuffleDescriptors) {
        this.executionId = executionId;
        this.serializedJobInformation = serializedJobInformation;
        this.taskInfo = taskInfo;
        this.jobID = jobID;
        this.partitionDeploymentConstraint = partitionDeploymentConstraint;
        this.consumedPartitionGroups = consumedPartitionGroups;
        this.resultPartitionRetriever = resultPartitionRetriever;
        this.blobWriter = blobWriter;
        this.consumedClusterPartitionShuffleDescriptors =
                consumedClusterPartitionShuffleDescriptors;
    }

    public TaskDeploymentDescriptor createDeploymentDescriptor(
            AllocationID allocationID,
            @Nullable JobManagerTaskRestore taskRestore,
            Collection<ResultPartitionDeploymentDescriptor> producedPartitions)
            throws IOException {
        return new TaskDeploymentDescriptor(
                jobID,
                serializedJobInformation,
                taskInfo,
                executionId,
                allocationID,
                taskRestore,
                new ArrayList<>(producedPartitions),
                createInputGateDeploymentDescriptors());
    }

    private List<InputGateDeploymentDescriptor> createInputGateDeploymentDescriptors()
            throws IOException {
        List<InputGateDeploymentDescriptor> inputGates =
                new ArrayList<>(consumedPartitionGroups.size());

        for (ConsumedPartitionGroup consumedPartitionGroup : consumedPartitionGroups) {
            // If the produced partition has multiple consumers registered, we
            // need to request the one matching our sub task index.
            // TODO Refactor after removing the consumers from the intermediate result partitions
            IntermediateResultPartition resultPartition =
                    resultPartitionRetriever.apply(consumedPartitionGroup.getFirst());

            IntermediateResult consumedIntermediateResult = resultPartition.getIntermediateResult();
            SubpartitionIndexRange consumedSubpartitionRange =
                    computeConsumedSubpartitionRange(
                            resultPartition, executionId.getSubtaskIndex());

            IntermediateDataSetID resultId = consumedIntermediateResult.getId();
            ResultPartitionType partitionType = consumedIntermediateResult.getResultType();

            inputGates.add(
                    new InputGateDeploymentDescriptor(
                            resultId,
                            partitionType,
                            consumedSubpartitionRange,
                            getConsumedPartitionShuffleDescriptors(
                                    consumedIntermediateResult, consumedPartitionGroup)));
        }

        for (Map.Entry<IntermediateDataSetID, ShuffleDescriptor[]> entry :
                consumedClusterPartitionShuffleDescriptors.entrySet()) {
            inputGates.add(
                    new InputGateDeploymentDescriptor(
                            entry.getKey(),
                            ResultPartitionType.BLOCKING_PERSISTENT,
                            0,
                            entry.getValue()));
        }

        return inputGates;
    }

    public static SubpartitionIndexRange computeConsumedSubpartitionRange(
            IntermediateResultPartition resultPartition, int consumerSubtaskIndex) {
        int numConsumers = resultPartition.getConsumerVertexGroup().size();
        int consumerIndex = consumerSubtaskIndex % numConsumers;
        IntermediateResult consumedIntermediateResult = resultPartition.getIntermediateResult();
        int numSubpartitions = resultPartition.getNumberOfSubpartitions();
        return computeConsumedSubpartitionRange(
                consumerIndex,
                numConsumers,
                numSubpartitions,
                consumedIntermediateResult.getProducer().getGraph().isDynamic(),
                consumedIntermediateResult.isBroadcast());
    }

    @VisibleForTesting
    static SubpartitionIndexRange computeConsumedSubpartitionRange(
            int consumerIndex,
            int numConsumers,
            int numSubpartitions,
            boolean isDynamicGraph,
            boolean isBroadcast) {

        if (!isDynamicGraph) {
            checkArgument(numConsumers == numSubpartitions);
            return new SubpartitionIndexRange(consumerIndex, consumerIndex);
        } else {
            if (isBroadcast) {
                // broadcast result should have only one subpartition, and be consumed multiple
                // times.
                checkArgument(numSubpartitions == 1);
                return new SubpartitionIndexRange(0, 0);
            } else {
                checkArgument(consumerIndex < numConsumers);
                checkArgument(numConsumers <= numSubpartitions);

                int start = consumerIndex * numSubpartitions / numConsumers;
                int nextStart = (consumerIndex + 1) * numSubpartitions / numConsumers;

                return new SubpartitionIndexRange(start, nextStart - 1);
            }
        }
    }

    private MaybeOffloaded<ShuffleDescriptor[]> getConsumedPartitionShuffleDescriptors(
            IntermediateResult intermediateResult, ConsumedPartitionGroup consumedPartitionGroup)
            throws IOException {
        MaybeOffloaded<ShuffleDescriptor[]> serializedShuffleDescriptors =
                intermediateResult.getCachedShuffleDescriptors(consumedPartitionGroup);
        if (serializedShuffleDescriptors == null) {
            serializedShuffleDescriptors =
                    computeConsumedPartitionShuffleDescriptors(consumedPartitionGroup);
            intermediateResult.cacheShuffleDescriptors(
                    consumedPartitionGroup, serializedShuffleDescriptors);
        }
        return serializedShuffleDescriptors;
    }

    private MaybeOffloaded<ShuffleDescriptor[]> computeConsumedPartitionShuffleDescriptors(
            ConsumedPartitionGroup consumedPartitionGroup) throws IOException {

        ShuffleDescriptor[] shuffleDescriptors =
                new ShuffleDescriptor[consumedPartitionGroup.size()];
        // Each edge is connected to a different result partition
        int i = 0;
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            shuffleDescriptors[i++] =
                    getConsumedPartitionShuffleDescriptor(
                            resultPartitionRetriever.apply(partitionId),
                            partitionDeploymentConstraint);
        }
        return serializeAndTryOffloadShuffleDescriptors(shuffleDescriptors);
    }

    private MaybeOffloaded<ShuffleDescriptor[]> serializeAndTryOffloadShuffleDescriptors(
            ShuffleDescriptor[] shuffleDescriptors) throws IOException {

        final CompressedSerializedValue<ShuffleDescriptor[]> compressedSerializedValue =
                CompressedSerializedValue.fromObject(shuffleDescriptors);

        final Either<SerializedValue<ShuffleDescriptor[]>, PermanentBlobKey>
                serializedValueOrBlobKey =
                        BlobWriter.tryOffload(compressedSerializedValue, jobID, blobWriter);

        if (serializedValueOrBlobKey.isLeft()) {
            return new TaskDeploymentDescriptor.NonOffloaded<>(serializedValueOrBlobKey.left());
        } else {
            return new TaskDeploymentDescriptor.Offloaded<>(serializedValueOrBlobKey.right());
        }
    }

    public static TaskDeploymentDescriptorFactory fromExecutionVertex(
            ExecutionVertex executionVertex) throws IOException, CacheCorruptedException {
        InternalExecutionGraphAccessor internalExecutionGraphAccessor =
                executionVertex.getExecutionGraphAccessor();
        Map<IntermediateDataSetID, ShuffleDescriptor[]> clusterPartitionShuffleDescriptors;
        try {
            clusterPartitionShuffleDescriptors =
                    getClusterPartitionShuffleDescriptors(executionVertex);
        } catch (Throwable e) {
            throw new CacheCorruptedException(
                    "Fail to get ShuffleDescriptors",
                    e,
                    executionVertex
                            .getJobVertex()
                            .getJobVertex()
                            .getIntermediateDataSetIdsToConsume());
        }

        return new TaskDeploymentDescriptorFactory(
                executionVertex.getCurrentExecutionAttempt().getAttemptId(),
                getSerializedJobInformation(internalExecutionGraphAccessor),
                getSerializedTaskInformation(
                        executionVertex.getJobVertex().getTaskInformationOrBlobKey()),
                internalExecutionGraphAccessor.getJobID(),
                internalExecutionGraphAccessor.getPartitionLocationConstraint(),
                executionVertex.getAllConsumedPartitionGroups(),
                internalExecutionGraphAccessor::getResultPartitionOrThrow,
                internalExecutionGraphAccessor.getBlobWriter(),
                clusterPartitionShuffleDescriptors);
    }

    private static Map<IntermediateDataSetID, ShuffleDescriptor[]>
            getClusterPartitionShuffleDescriptors(ExecutionVertex executionVertex) {
        final InternalExecutionGraphAccessor internalExecutionGraphAccessor =
                executionVertex.getExecutionGraphAccessor();
        final List<IntermediateDataSetID> consumedClusterDataSetIds =
                executionVertex.getJobVertex().getJobVertex().getIntermediateDataSetIdsToConsume();
        Map<IntermediateDataSetID, ShuffleDescriptor[]> clusterPartitionShuffleDescriptors =
                new HashMap<>();

        for (IntermediateDataSetID consumedClusterDataSetId : consumedClusterDataSetIds) {
            Collection<? extends ShuffleDescriptor> shuffleDescriptors =
                    internalExecutionGraphAccessor.getClusterPartitionShuffleDescriptors(
                            consumedClusterDataSetId);

            Preconditions.checkState(
                    executionVertex.getTotalNumberOfParallelSubtasks() == shuffleDescriptors.size(),
                    "The parallelism (%s) of the cache consuming job vertex is "
                            + "different from the number of shuffle descriptors (%s) of the intermediate data set",
                    executionVertex.getTotalNumberOfParallelSubtasks(),
                    shuffleDescriptors.size());

            shuffleDescriptors =
                    shuffleDescriptors.stream()
                            .filter(
                                    descriptor ->
                                            descriptor
                                                            .getResultPartitionID()
                                                            .getPartitionId()
                                                            .getPartitionNumber()
                                                    == executionVertex.getParallelSubtaskIndex())
                            .collect(Collectors.toList());

            Preconditions.checkState(
                    shuffleDescriptors.size() == 1,
                    "Expecting one shuffle descriptor but got %s",
                    shuffleDescriptors.size());
            clusterPartitionShuffleDescriptors.put(
                    consumedClusterDataSetId, shuffleDescriptors.toArray(new ShuffleDescriptor[0]));
        }
        return clusterPartitionShuffleDescriptors;
    }

    private static MaybeOffloaded<JobInformation> getSerializedJobInformation(
            InternalExecutionGraphAccessor internalExecutionGraphAccessor) {
        Either<SerializedValue<JobInformation>, PermanentBlobKey> jobInformationOrBlobKey =
                internalExecutionGraphAccessor.getJobInformationOrBlobKey();
        if (jobInformationOrBlobKey.isLeft()) {
            return new TaskDeploymentDescriptor.NonOffloaded<>(jobInformationOrBlobKey.left());
        } else {
            return new TaskDeploymentDescriptor.Offloaded<>(jobInformationOrBlobKey.right());
        }
    }

    private static MaybeOffloaded<TaskInformation> getSerializedTaskInformation(
            Either<SerializedValue<TaskInformation>, PermanentBlobKey> taskInfo) {
        return taskInfo.isLeft()
                ? new TaskDeploymentDescriptor.NonOffloaded<>(taskInfo.left())
                : new TaskDeploymentDescriptor.Offloaded<>(taskInfo.right());
    }

    public static ShuffleDescriptor getConsumedPartitionShuffleDescriptor(
            IntermediateResultPartition consumedPartition,
            PartitionLocationConstraint partitionDeploymentConstraint) {
        Execution producer = consumedPartition.getProducer().getCurrentExecutionAttempt();

        ExecutionState producerState = producer.getState();
        Optional<ResultPartitionDeploymentDescriptor> consumedPartitionDescriptor =
                producer.getResultPartitionDeploymentDescriptor(consumedPartition.getPartitionId());

        ResultPartitionID consumedPartitionId =
                new ResultPartitionID(consumedPartition.getPartitionId(), producer.getAttemptId());

        return getConsumedPartitionShuffleDescriptor(
                consumedPartitionId,
                consumedPartition.getResultType(),
                consumedPartition.isConsumable(),
                producerState,
                partitionDeploymentConstraint,
                consumedPartitionDescriptor.orElse(null));
    }

    @VisibleForTesting
    static ShuffleDescriptor getConsumedPartitionShuffleDescriptor(
            ResultPartitionID consumedPartitionId,
            ResultPartitionType resultPartitionType,
            boolean isConsumable,
            ExecutionState producerState,
            PartitionLocationConstraint partitionDeploymentConstraint,
            @Nullable ResultPartitionDeploymentDescriptor consumedPartitionDescriptor) {
        // The producing task needs to be RUNNING or already FINISHED
        if ((resultPartitionType.isPipelined() || isConsumable)
                && consumedPartitionDescriptor != null
                && isProducerAvailable(producerState)) {
            // partition is already registered
            return consumedPartitionDescriptor.getShuffleDescriptor();
        } else if (partitionDeploymentConstraint == PartitionLocationConstraint.CAN_BE_UNKNOWN) {
            // The producing task might not have registered the partition yet
            //
            // Currently, UnknownShuffleDescriptor will be created only if there is an intra-region
            // blocking edge in the graph. This means that when its consumer restarts, the
            // producer of the UnknownShuffleDescriptors will also restart. Therefore, it's safe to
            // cache UnknownShuffleDescriptors and there's no need to update the cache when the
            // corresponding partition becomes consumable.
            return new UnknownShuffleDescriptor(consumedPartitionId);
        } else {
            // throw respective exceptions
            throw handleConsumedPartitionShuffleDescriptorErrors(
                    consumedPartitionId, resultPartitionType, isConsumable, producerState);
        }
    }

    private static RuntimeException handleConsumedPartitionShuffleDescriptorErrors(
            ResultPartitionID consumedPartitionId,
            ResultPartitionType resultPartitionType,
            boolean isConsumable,
            ExecutionState producerState) {
        String msg;
        if (isProducerFailedOrCanceled(producerState)) {
            msg =
                    "Trying to consume an input partition whose producer has been canceled or failed. "
                            + "The producer is in state "
                            + producerState
                            + ".";
        } else {
            msg =
                    String.format(
                            "Trying to consume an input partition whose producer "
                                    + "is not ready (result type: %s, partition consumable: %s, producer state: %s, partition id: %s).",
                            resultPartitionType, isConsumable, producerState, consumedPartitionId);
        }
        return new IllegalStateException(msg);
    }

    private static boolean isProducerAvailable(ExecutionState producerState) {
        return producerState == ExecutionState.RUNNING
                || producerState == ExecutionState.INITIALIZING
                || producerState == ExecutionState.FINISHED
                || producerState == ExecutionState.SCHEDULED
                || producerState == ExecutionState.DEPLOYING;
    }

    private static boolean isProducerFailedOrCanceled(ExecutionState producerState) {
        return producerState == ExecutionState.CANCELING
                || producerState == ExecutionState.CANCELED
                || producerState == ExecutionState.FAILED;
    }

    /**
     * Defines whether the partition's location must be known at deployment time or can be unknown
     * and, therefore, updated later.
     */
    public enum PartitionLocationConstraint {
        MUST_BE_KNOWN,
        CAN_BE_UNKNOWN;

        public static PartitionLocationConstraint fromJobType(JobType jobType) {
            switch (jobType) {
                case BATCH:
                    return CAN_BE_UNKNOWN;
                case STREAMING:
                    return MUST_BE_KNOWN;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unknown JobType %s. Cannot derive partition location constraint for it.",
                                    jobType));
            }
        }
    }
}
