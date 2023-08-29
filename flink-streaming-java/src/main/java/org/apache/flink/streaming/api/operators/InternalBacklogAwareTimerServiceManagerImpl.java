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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.streaming.api.operators.sorted.state.BacklogExecutionInternalTimeService;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTaskCancellationContext;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingRuntimeException;

import java.util.HashMap;
import java.util.Map;

/** InternalBacklogAwareTimerServiceManagerImpl. */
public class InternalBacklogAwareTimerServiceManagerImpl<K>
        extends InternalTimeServiceManagerImpl<K>
        implements InternalTimeServiceManager<K>, KeyedStateBackend.KeySelectionListener<K> {

    private final Map<String, InternalBacklogAwareTimerServiceImpl<K, ?>> timerServices =
            new HashMap<>();

    private boolean backlog = false;

    InternalBacklogAwareTimerServiceManagerImpl(
            KeyGroupRange localKeyGroupRange,
            KeyContext keyContext,
            PriorityQueueSetFactory priorityQueueSetFactory,
            ProcessingTimeService processingTimeService,
            StreamTaskCancellationContext cancellationContext) {
        super(
                localKeyGroupRange,
                keyContext,
                priorityQueueSetFactory,
                processingTimeService,
                cancellationContext);
    }

    @Override
    public <N> InternalTimerService<N> getInternalTimerService(
            String name,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            Triggerable<K, N> triggerable) {

        final InternalTimerServiceImpl<K, N> internalTimerService =
                (InternalTimerServiceImpl<K, N>)
                        super.getInternalTimerService(
                                name, keySerializer, namespaceSerializer, triggerable);
        final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue =
                internalTimerService.getEventTimeTimersQueue();
        final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>>
                processingTimeTimersQueue = internalTimerService.getProcessingTimeTimersQueue();

        final BacklogExecutionInternalTimeService<K, N> batchExecutionInternalTimerService =
                new BacklogExecutionInternalTimeService<>(
                        processingTimeService,
                        triggerable,
                        eventTimeTimersQueue,
                        processingTimeTimersQueue);

        InternalBacklogAwareTimerServiceImpl<K, N> timerService =
                (InternalBacklogAwareTimerServiceImpl<K, N>) timerServices.get(name);
        if (timerService == null) {
            timerService =
                    new InternalBacklogAwareTimerServiceImpl<>(
                            internalTimerService, batchExecutionInternalTimerService);
            timerServices.put(name, timerService);
        }

        return timerService;
    }

    @Override
    public void advanceWatermark(Watermark watermark) throws Exception {
        //        Preconditions.checkState(!backlog, "Watermark cannot advanced during backlog.");
        for (InternalBacklogAwareTimerServiceImpl<?, ?> service : timerServices.values()) {
            service.advanceWatermark(watermark.getTimestamp());
        }
    }

    @Override
    public void snapshotToRawKeyedState(
            KeyedStateCheckpointOutputStream stateCheckpointOutputStream, String operatorName)
            throws Exception {
        Preconditions.checkState(!backlog, "Cannot snapshot state during backlog.");
        super.snapshotToRawKeyedState(stateCheckpointOutputStream, operatorName);
    }

    public static <K> InternalBacklogAwareTimerServiceManagerImpl<K> create(
            CheckpointableKeyedStateBackend<K> keyedStateBackend,
            ClassLoader userClassloader,
            KeyContext keyContext,
            ProcessingTimeService processingTimeService,
            Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStates,
            StreamTaskCancellationContext cancellationContext)
            throws Exception {

        final InternalBacklogAwareTimerServiceManagerImpl<K> manager =
                new InternalBacklogAwareTimerServiceManagerImpl<>(
                        keyedStateBackend.getKeyGroupRange(),
                        keyContext,
                        keyedStateBackend,
                        processingTimeService,
                        cancellationContext);

        keyedStateBackend.registerKeySelectionListener(manager);

        return manager;
    }

    @Override
    public void keySelected(K newKey) {
        try {
            for (InternalBacklogAwareTimerServiceImpl<K, ?> value : timerServices.values()) {
                value.setCurrentKey(newKey);
            }
        } catch (Exception e) {
            throw new WrappingRuntimeException(e);
        }
    }

    public void setBacklog(boolean backlog) {
        try {
            for (InternalBacklogAwareTimerServiceImpl<K, ?> value : timerServices.values()) {
                value.setBacklog(backlog);
            }
            this.backlog = backlog;
        } catch (Exception e) {
            throw new WrappingRuntimeException(e);
        }
    }
}
