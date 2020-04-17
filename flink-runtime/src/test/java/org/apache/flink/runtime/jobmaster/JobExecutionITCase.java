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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.runtime.RuntimeSerializerFactory;
import org.apache.flink.runtime.executiongraph.ClusterPartitionDescriptorImpl;
import org.apache.flink.runtime.executiongraph.ClusterPartitionReport;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.testutils.junit.category.AlsoRunWithLegacyScheduler;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Integration tests for job scheduling.
 */
@Category(AlsoRunWithLegacyScheduler.class)
public class JobExecutionITCase extends TestLogger {

	/**
	 * Tests that tasks with a co-location constraint are scheduled in the same
	 * slots. In fact it also tests that consumers are scheduled wrt their input
	 * location if the co-location constraint is deactivated.
	 */
	@Test
	public void testCoLocationConstraintJobExecution() throws Exception {
		final int numSlotsPerTaskExecutor = 1;
		final int numTaskExecutors = 3;
		final int parallelism = numTaskExecutors * numSlotsPerTaskExecutor;
		final JobGraph jobGraph = createJobGraph(parallelism);

		final TestingMiniClusterConfiguration miniClusterConfiguration = new TestingMiniClusterConfiguration.Builder()
			.setNumSlotsPerTaskManager(numSlotsPerTaskExecutor)
			.setNumTaskManagers(numTaskExecutors)
			.setLocalCommunication(true)
			.build();

		try (TestingMiniCluster miniCluster = new TestingMiniCluster(miniClusterConfiguration)) {
			miniCluster.start();

			miniCluster.submitJob(jobGraph).get();

			final CompletableFuture<JobResult> jobResultFuture = miniCluster.requestJobResult(jobGraph.getJobID());

			assertThat(jobResultFuture.get().isSuccess(), is(true));
		}
	}

	@Test
	public void testClusterPartitionReuse() throws Exception {
		final TestingMiniClusterConfiguration miniClusterConfiguration = new TestingMiniClusterConfiguration.Builder()
			.setReleasePartitionDuringExecution(false)
			.build();

		try (TestingMiniCluster miniCluster = new TestingMiniCluster(miniClusterConfiguration)) {
			miniCluster.start();

			final JobGraph firstJobGraph = createFirstJobGraph();
			miniCluster.submitJob(firstJobGraph).get();
			final CompletableFuture<JobResult> jobResultFuture =
				miniCluster.requestJobResult(firstJobGraph.getJobID());
			final JobResult jobResult = jobResultFuture.get();
			assertThat(jobResult.isSuccess(), is(true));

			ClusterPartitionReport clusterPartitionReport = jobResult.getClusterPartitionReport();
			assertNotNull(clusterPartitionReport);

			final JobGraph secondJobGraph =
				createSecondJobGraph(clusterPartitionReport.getClusterPartitionDescriptor());
			miniCluster.submitJob(secondJobGraph).get();
			assertThat(miniCluster.requestJobResult(secondJobGraph.getJobID()).get().isSuccess(),
				is(true));
		}
	}

	@Test
	public void testClusterPartitionReuseParallelism() throws Exception {
		final TestingMiniClusterConfiguration miniClusterConfiguration = new TestingMiniClusterConfiguration.Builder()
			.setReleasePartitionDuringExecution(false)
			.build();

		try (TestingMiniCluster miniCluster = new TestingMiniCluster(miniClusterConfiguration)) {
			miniCluster.start();

			final JobGraph firstJobGraph = createFirstJobGraph(2,3);
			miniCluster.submitJob(firstJobGraph).get();
			final CompletableFuture<JobResult> jobResultFuture =
				miniCluster.requestJobResult(firstJobGraph.getJobID());
			final JobResult jobResult = jobResultFuture.get();
			assertThat(jobResult.isSuccess(), is(true));

			ClusterPartitionReport clusterPartitionReport = jobResult.getClusterPartitionReport();
			assertNotNull(clusterPartitionReport);

			final JobGraph secondJobGraph =
				createSecondJobGraph(clusterPartitionReport.getClusterPartitionDescriptor());
			miniCluster.submitJob(secondJobGraph).get();
			assertThat(miniCluster.requestJobResult(secondJobGraph.getJobID()).get().isSuccess(),
				is(true));
		}
	}

	private JobGraph createFirstJobGraph() {
		return createFirstJobGraph(1, 1);
	}

	private JobGraph createSecondJobGraph(Collection<ClusterPartitionDescriptorImpl> clusterPartitions) {
		final JobVertex noop = new JobVertex("No op");
		noop.connectClusterPartitionInput(clusterPartitions);
		noop.setInvokableClass(BatchTask.class);
		TaskConfig config = new TaskConfig(noop.getConfiguration());
		DriverStrategy ds = DriverStrategy.UNARY_NO_OP;
		config.setDriver(ds.getDriverClass());
		config.setDriverStrategy(ds);
		config.addInputToGroup(0);
		config.setInputSerializer(new RuntimeSerializerFactory<>(new IntSerializer(), Integer.class), 0);
		config.setOutputSerializer(new RuntimeSerializerFactory<>(new IntSerializer(), Integer.class));
		config.addOutputShipStrategy(ShipStrategyType.PARTITION_RANDOM);

		final JobVertex receiver = new JobVertex("Receiver 2");
		receiver.setParallelism(5);
		receiver.setInvokableClass(TestingAbstractInvokables.PrintReceiver.class);

		receiver.connectNewDataSetAsInput(noop, DistributionPattern.POINTWISE,
			ResultPartitionType.PIPELINED);

		return new JobGraph("Second Job", noop, receiver);
	}



	private JobGraph createFirstJobGraph(int senderParallelism, int receiverParallelism) {
		final JobVertex sender = new JobVertex("Sender");
		sender.setParallelism(senderParallelism);
		sender.setInvokableClass(TestingAbstractInvokables.Sender.class);

		final JobVertex receiver = new JobVertex("Receiver");
		receiver.setParallelism(receiverParallelism);
		receiver.setInvokableClass(TestingAbstractInvokables.PrintReceiver.class);

		receiver.connectNewDataSetAsInput(sender,
			DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING_PERSISTENT);

		return new JobGraph("First Job", sender, receiver);
	}

	private JobGraph createJobGraph(int parallelism) {
		final JobVertex sender = new JobVertex("Sender");
		sender.setParallelism(parallelism);
		sender.setInvokableClass(TestingAbstractInvokables.Sender.class);

		final JobVertex receiver = new JobVertex("Receiver");
		receiver.setParallelism(parallelism);
		receiver.setInvokableClass(TestingAbstractInvokables.Receiver.class);

		// In order to make testCoLocationConstraintJobExecution fail, one needs to
		// remove the co-location constraint and the slot sharing groups, because then
		// the receivers will have to wait for the senders to finish and the slot
		// assignment order to the receivers is non-deterministic (depending on the
		// order in which the senders finish).
		final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
		receiver.setSlotSharingGroup(slotSharingGroup);
		sender.setSlotSharingGroup(slotSharingGroup);
		receiver.setStrictlyCoLocatedWith(sender);

		receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(getClass().getSimpleName(), sender, receiver);

		return jobGraph;
	}
}
