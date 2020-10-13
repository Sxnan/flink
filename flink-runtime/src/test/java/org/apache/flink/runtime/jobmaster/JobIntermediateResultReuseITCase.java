package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.PersistedIntermediateResultDescriptor;
import org.apache.flink.runtime.executiongraph.PersistedIntermediateResultDescriptorImpl;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Integration tests for reusing persisted intermediate result
 */
public class JobIntermediateResultReuseITCase {

	@Test
	public void testClusterPartitionReuse() throws Exception {
		final TestingMiniClusterConfiguration miniClusterConfiguration = new TestingMiniClusterConfiguration.Builder()
			.build();

		try (TestingMiniCluster miniCluster = new TestingMiniCluster(miniClusterConfiguration)) {
			miniCluster.start();

			final JobGraph firstJobGraph = createFirstJobGraph(1, 1);
			miniCluster.submitJob(firstJobGraph).get();
			final CompletableFuture<JobResult> jobResultFuture =
				miniCluster.requestJobResult(firstJobGraph.getJobID());
			final JobResult jobResult = jobResultFuture.get();
			assertThat(jobResult.isSuccess(), is(true));

			final Map<IntermediateDataSetID, PersistedIntermediateResultDescriptor> persistedIntermediateResult =
				jobResult.getPersistedIntermediateResult();
			assertNotNull(persistedIntermediateResult);

			final JobGraph secondJobGraph =
				createSecondJobGraph(persistedIntermediateResult.values().iterator().next());
			miniCluster.submitJob(secondJobGraph).get();
			assertThat(miniCluster.requestJobResult(secondJobGraph.getJobID()).get().isSuccess(),
				is(true));
		}
	}

	private JobGraph createSecondJobGraph(PersistedIntermediateResultDescriptor clusterPartitions) {
		final JobVertex receiver = new JobVertex("Receiver 2");
		receiver.setParallelism(1);
		receiver.setIntermediateResultInput((PersistedIntermediateResultDescriptorImpl) clusterPartitions);
		receiver.setInvokableClass(TestingAbstractInvokables.Receiver.class);

		return new JobGraph("Second Job", receiver);
	}

	private JobGraph createFirstJobGraph(int senderParallelism, int receiverParallelism) {
		final JobVertex sender = new JobVertex("Sender");
		sender.setParallelism(senderParallelism);
		sender.setInvokableClass(TestingAbstractInvokables.Sender.class);

		final JobVertex receiver = new JobVertex("Receiver");
		receiver.setParallelism(receiverParallelism);
		receiver.setInvokableClass(TestingAbstractInvokables.Receiver.class);

		receiver.connectNewDataSetAsInput(sender,
			DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING_PERSISTENT);

		return new JobGraph("First Job", sender, receiver);
	}

}
