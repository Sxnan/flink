/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertTrue;

/** Integration tests for reusing persisted intermediate result */
public class JobIntermediateResultReuseITCase {

    @Test
    public void testClusterPartitionReuse() throws Exception {
        final TestingMiniClusterConfiguration miniClusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder().build();

        try (TestingMiniCluster miniCluster =
                TestingMiniCluster.newBuilder(miniClusterConfiguration).build()) {
            miniCluster.start();

            final JobGraph firstJobGraph = createFirstJobGraph(1, 1);
            miniCluster.submitJob(firstJobGraph).get();
            final CompletableFuture<JobResult> jobResultFuture =
                    miniCluster.requestJobResult(firstJobGraph.getJobID());
            final JobResult jobResult = jobResultFuture.get();
            assertTrue(jobResult.isSuccess());

            //            final Map<IntermediateDataSetID, PersistedIntermediateResultDescriptor>
            // persistedIntermediateResult =
            //                    jobResult.getPersistedIntermediateResult();
            //            assertNotNull(persistedIntermediateResult);
            //
            //            final JobGraph secondJobGraph =
            //
            // createSecondJobGraph(persistedIntermediateResult.values().iterator().next());
            //            miniCluster.submitJob(secondJobGraph).get();
            //            assertThat(
            //
            // miniCluster.requestJobResult(secondJobGraph.getJobID()).get().isSuccess(),
            //                    is(true));
        }
    }

    //    private JobGraph createSecondJobGraph(PersistedIntermediateResultDescriptor
    // clusterPartitions) {
    //        final JobVertex receiver = new JobVertex("Receiver 2");
    //        receiver.setParallelism(1);
    //        receiver.setIntermediateResultInput((PersistedIntermediateResultDescriptorImpl)
    // clusterPartitions);
    //        receiver.setInvokableClass(TestingAbstractInvokables.Receiver.class);
    //
    //        return new JobGraph("Second Job", receiver);
    //    }

    private JobGraph createFirstJobGraph(int senderParallelism, int receiverParallelism) {
        final JobVertex sender = new JobVertex("Sender");
        sender.setParallelism(senderParallelism);
        sender.setInvokableClass(TestingAbstractInvokables.Sender.class);

        final JobVertex receiver = new JobVertex("Receiver");
        receiver.setParallelism(receiverParallelism);
        receiver.setInvokableClass(TestingAbstractInvokables.Receiver.class);

        final JobVertex dummy_receiver = new JobVertex("Receiver");
        dummy_receiver.setParallelism(receiverParallelism);
        dummy_receiver.setInvokableClass(TestingAbstractInvokables.Receiver.class);

        receiver.connectNewDataSetAsInput(
                sender, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING_PERSISTENT);
        dummy_receiver.connectNewDataSetAsInput(
                sender, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING_PERSISTENT);

        return new JobGraph(null, "First Job", sender, receiver);
    }
}
