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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.PersistedIntermediateDataSetDescriptor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlotProvider;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ExecutionGraphGenerateClusterPartitionDescriptorTest extends TestLogger {

    private static final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor();
    private static final ComponentMainThreadExecutor mainThreadExecutor =
            ComponentMainThreadExecutorServiceAdapter.forMainThread();

    @Test
    public void testGenerateClusterPartitionDescriptor() throws Exception {
        final JobVertex sourceVertex = ExecutionGraphTestUtils.createNoOpVertex("source", 1);
        final JobVertex operatorVertex = ExecutionGraphTestUtils.createNoOpVertex("noop", 1);
        final JobVertex sinkVertex = ExecutionGraphTestUtils.createNoOpVertex("sink", 1);

        operatorVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.POINTWISE,
                ResultPartitionType.BLOCKING);
        sinkVertex.connectNewDataSetAsInput(operatorVertex, DistributionPattern.POINTWISE,
                ResultPartitionType.BLOCKING_PERSISTENT);

        final JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(
                sourceVertex,
                operatorVertex,
                sinkVertex);
        final DefaultScheduler scheduler = SchedulerTestingUtils
                .newSchedulerBuilder(jobGraph, mainThreadExecutor)
                .setExecutionSlotAllocatorFactory(
                        SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                TestingPhysicalSlotProvider.create(
                                        (ignored) ->
                                                CompletableFuture.completedFuture(
                                                        TestingPhysicalSlot.builder()
                                                                .build()))))
                .build();

        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();
        scheduler.startScheduling();


        executionGraph.getAllExecutionVertices()
                .forEach(executionVertex -> {
                    scheduler.updateTaskExecutionState(new TaskExecutionStateTransition(
                            new TaskExecutionState(executionVertex
                                    .getCurrentExecutionAttempt()
                                    .getAttemptId(), ExecutionState.FINISHED)
                    ));
                });

        final Map<IntermediateDataSetID, PersistedIntermediateDataSetDescriptor> persistedIntermediateResult = executionGraph
                .getPersistedIntermediateResult();



    }

//    @Test
//    public void testGenerateClusterPartitionDescriptorBeforeFinishReturnNull() throws Exception {
//        final JobVertex sourceVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
//        final JobVertex operatorVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
//        final JobVertex sinkVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
//
//        operatorVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.POINTWISE,
//                ResultPartitionType.BLOCKING_PERSISTENT);
//        sinkVertex.connectNewDataSetAsInput(operatorVertex, DistributionPattern.POINTWISE,
//                ResultPartitionType.BLOCKING_PERSISTENT);
//
//        final ExecutionGraph executionGraph =
//                createExecutionGraph(sourceVertex, operatorVertex, sinkVertex);
//
//        assertNull(executionGraph.getPersistedIntermediateResult());
//    }

//    private ExecutionGraph createExecutionGraph(final JobVertex... vertices) throws Exception {
//        DefaultExecutionGraphBuilder.buildGraph()
//        final ExecutionGraph executionGraph = DefaultExecutionGraphBuilder.buildGraph(
//                new JobGraph(new JobID(), "test job", vertices),
//                new Configuration(),
//                scheduledExecutorService,
//                mainThreadExecutor.getMainThreadExecutor(),
//                new TestingSlotProvider(ignored -> CompletableFuture.completedFuture(
//                        new TestingLogicalSlotBuilder().createTestingLogicalSlot())),
//                ExecutionGraphPartitionReleaseTest.class.getClassLoader(),
//                new StandaloneCheckpointRecoveryFactory(),
//                AkkaUtils.getDefaultTimeout(),
//                new NoRestartStrategy(),
//                new UnregisteredMetricsGroup(),
//                VoidBlobWriter.getInstance(),
//                AkkaUtils.getDefaultTimeout(),
//                log,
//                NettyShuffleMaster.INSTANCE,
//                NoOpJobMasterPartitionTracker.INSTANCE,
//                System.currentTimeMillis());
//
//        executionGraph.start(mainThreadExecutor.getMainThreadExecutor());
//        mainThreadExecutor.execute(executionGraph::scheduleForExecution);
//
//        return executionGraph;
//    }
}
