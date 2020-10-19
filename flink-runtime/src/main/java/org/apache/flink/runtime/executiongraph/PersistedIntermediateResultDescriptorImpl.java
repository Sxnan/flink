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

import org.apache.flink.api.common.PersistedIntermediateResultDescriptor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.ArrayList;
import java.util.Collection;

public class PersistedIntermediateResultDescriptorImpl implements PersistedIntermediateResultDescriptor {
	private Collection<ClusterPartitionDescriptorImpl> clusterPartitionDescriptors = new ArrayList<>();
	private final IntermediateDataSetID intermediateDataSetID;
	private final ResultPartitionType resultPartitionType;

	public PersistedIntermediateResultDescriptorImpl(IntermediateDataSetID intermediateDataSetID,
													 ResultPartitionType resultPartitionType) {
		this.intermediateDataSetID = intermediateDataSetID;
		this.resultPartitionType = resultPartitionType;
	}

	public Collection<ClusterPartitionDescriptorImpl> getClusterPartitionDescriptors() {
		return clusterPartitionDescriptors;
	}

	public ResultPartitionType getResultPartitionType() {
		return resultPartitionType;
	}

	@Override
	public IntermediateDataSetID getIntermediateDataSetId() {
		return intermediateDataSetID;
	}

	public void addClusterPartitionDescriptor(ClusterPartitionDescriptorImpl clusterPartitionDescriptor) {
		clusterPartitionDescriptors.add(clusterPartitionDescriptor);
	}
}
