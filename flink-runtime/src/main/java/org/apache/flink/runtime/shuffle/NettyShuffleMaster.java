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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.LocalExecutionPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.NetworkPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.PartitionConnectionInfo;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default {@link ShuffleMaster} for netty and local file based shuffle implementation. */
public class NettyShuffleMaster implements ShuffleMaster<NettyShuffleDescriptor> {

    private static final Logger LOG = LoggerFactory.getLogger(NettyShuffleMaster.class);

    private final int buffersPerInputChannel;

    private final int buffersPerInputGate;

    private final int sortShuffleMinParallelism;

    private final int sortShuffleMinBuffers;

    private final int networkBufferSize;

    private final Map<IntermediateDataSetID, Collection<NettyShuffleDescriptor>>
            clusterPartitionShuffleDescriptors;

    public NettyShuffleMaster(Configuration conf) {
        checkNotNull(conf);
        buffersPerInputChannel =
                conf.getInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
        buffersPerInputGate =
                conf.getInteger(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);
        sortShuffleMinParallelism =
                conf.getInteger(
                        NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM);
        sortShuffleMinBuffers =
                conf.getInteger(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS);
        networkBufferSize = ConfigurationParserUtils.getPageSize(conf);
        clusterPartitionShuffleDescriptors = new HashMap<>();
    }

    @Override
    public CompletableFuture<NettyShuffleDescriptor> registerPartitionWithProducer(
            JobID jobID,
            PartitionDescriptor partitionDescriptor,
            ProducerDescriptor producerDescriptor) {
        LOG.debug(
                "Register Partition for Job: {}, PartitionDescriptor: {}, ProducerDescriptor: {}",
                jobID,
                partitionDescriptor,
                producerDescriptor);
        ResultPartitionID resultPartitionID =
                new ResultPartitionID(
                        partitionDescriptor.getPartitionId(),
                        producerDescriptor.getProducerExecutionId());

        NettyShuffleDescriptor shuffleDeploymentDescriptor =
                new NettyShuffleDescriptor(
                        producerDescriptor.getProducerLocation(),
                        createConnectionInfo(
                                producerDescriptor, partitionDescriptor.getConnectionIndex()),
                        resultPartitionID);

        return CompletableFuture.completedFuture(shuffleDeploymentDescriptor);
    }

    @Override
    public Collection<NettyShuffleDescriptor> getClusterPartitionShuffleDescriptors(
            IntermediateDataSetID intermediateDataSetID) {
        return clusterPartitionShuffleDescriptors.get(intermediateDataSetID);
    }

    @Override
    public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
        LOG.debug("Release partition externally: {}", shuffleDescriptor.getResultPartitionID());
        Preconditions.checkState(
                shuffleDescriptor instanceof NettyShuffleDescriptor,
                "NettyShuffleMaster can only release NettyShuffleDescriptor but got {}",
                shuffleDescriptor.getClass().getCanonicalName());
        final IntermediateDataSetID dataSetID =
                shuffleDescriptor
                        .getResultPartitionID()
                        .getPartitionId()
                        .getIntermediateDataSetID();
        final Collection<NettyShuffleDescriptor> descriptors =
                clusterPartitionShuffleDescriptors.get(dataSetID);
        if (descriptors == null || !descriptors.contains(shuffleDescriptor)) {
            return;
        }
        descriptors.remove(shuffleDescriptor);
        if (descriptors.isEmpty()) {
            clusterPartitionShuffleDescriptors.remove(dataSetID);
        }
    }

    private static PartitionConnectionInfo createConnectionInfo(
            ProducerDescriptor producerDescriptor, int connectionIndex) {
        return producerDescriptor.getDataPort() >= 0
                ? NetworkPartitionConnectionInfo.fromProducerDescriptor(
                        producerDescriptor, connectionIndex)
                : LocalExecutionPartitionConnectionInfo.INSTANCE;
    }

    /**
     * JM announces network memory requirement from the calculating result of this method. Please
     * note that the calculating algorithm depends on both I/O details of a vertex and network
     * configuration, e.g. {@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_PER_CHANNEL} and
     * {@link NettyShuffleEnvironmentOptions#NETWORK_EXTRA_BUFFERS_PER_GATE}, which means we should
     * always keep the consistency of configurations between JM, RM and TM in fine-grained resource
     * management, thus to guarantee that the processes of memory announcing and allocating respect
     * each other.
     */
    @Override
    public MemorySize computeShuffleMemorySizeForTask(TaskInputsOutputsDescriptor desc) {
        checkNotNull(desc);

        int numTotalInputChannels =
                desc.getInputChannelNums().values().stream().mapToInt(Integer::intValue).sum();
        int numTotalInputGates = desc.getInputChannelNums().size();

        int numRequiredNetworkBuffers =
                NettyShuffleUtils.computeNetworkBuffersForAnnouncing(
                        buffersPerInputChannel,
                        buffersPerInputGate,
                        sortShuffleMinParallelism,
                        sortShuffleMinBuffers,
                        numTotalInputChannels,
                        numTotalInputGates,
                        desc.getSubpartitionNums(),
                        desc.getPartitionTypes());

        return new MemorySize((long) networkBufferSize * numRequiredNetworkBuffers);
    }

    @Override
    public void promotePartition(ShuffleDescriptor shuffleDescriptor) {
        LOG.debug(
                "Promote partition to cluster partition: {}",
                shuffleDescriptor.getResultPartitionID());
        Preconditions.checkState(
                shuffleDescriptor instanceof NettyShuffleDescriptor,
                "NettyShuffleMaster can only promote partition "
                        + "with NettyShuffleDescriptor but got %s",
                shuffleDescriptor.getClass().getCanonicalName());
        final IntermediateDataSetID dataSetID =
                shuffleDescriptor
                        .getResultPartitionID()
                        .getPartitionId()
                        .getIntermediateDataSetID();
        final Collection<NettyShuffleDescriptor> descriptors =
                clusterPartitionShuffleDescriptors.computeIfAbsent(
                        dataSetID, (key) -> new HashSet<>());
        descriptors.add((NettyShuffleDescriptor) shuffleDescriptor);
    }

    @Override
    public void removeClusterPartition(ShuffleDescriptor shuffleDescriptor) {
        LOG.debug("Remove cluster partition: {}", shuffleDescriptor.getResultPartitionID());
        Preconditions.checkState(
                shuffleDescriptor instanceof NettyShuffleDescriptor,
                "NettyShuffleMaster can only remove cluster partition "
                        + "with NettyShuffleDescriptor but got %s",
                shuffleDescriptor.getClass().getCanonicalName());
        final IntermediateDataSetID dataSetID =
                shuffleDescriptor
                        .getResultPartitionID()
                        .getPartitionId()
                        .getIntermediateDataSetID();
        final Collection<NettyShuffleDescriptor> descriptors =
                clusterPartitionShuffleDescriptors.computeIfAbsent(
                        dataSetID, (key) -> new HashSet<>());

        if (!descriptors.contains(shuffleDescriptor)) {
            LOG.warn("Removing non-exist cluster partition of IntermediateDataSet {}", dataSetID);
        }
        descriptors.remove(shuffleDescriptor);

        if (descriptors.isEmpty()) {
            clusterPartitionShuffleDescriptors.remove(dataSetID);
        }
    }
}
