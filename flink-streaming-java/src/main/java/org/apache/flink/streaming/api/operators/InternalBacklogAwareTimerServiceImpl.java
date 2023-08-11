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

import org.apache.flink.streaming.api.operators.sorted.state.BacklogExecutionInternalTimeService;
import org.apache.flink.util.function.BiConsumerWithException;

/** InternalBacklogAwareTimerServiceImpl. */
public class InternalBacklogAwareTimerServiceImpl<K, N> implements InternalTimerService<N> {

    private final InternalTimerServiceImpl<K, N> internalTimerService;
    private final BacklogExecutionInternalTimeService<K, N> batchExecutionInternalTimeService;
    private InternalTimerService<N> currentInternalTimerService;

    public InternalBacklogAwareTimerServiceImpl(
            InternalTimerServiceImpl<K, N> internalTimerService,
            BacklogExecutionInternalTimeService<K, N> batchExecutionInternalTimeService) {
        this.internalTimerService = internalTimerService;
        this.batchExecutionInternalTimeService = batchExecutionInternalTimeService;
        this.currentInternalTimerService = internalTimerService;
    }

    @Override
    public long currentProcessingTime() {
        return currentInternalTimerService.currentProcessingTime();
    }

    @Override
    public long currentWatermark() {
        return currentInternalTimerService.currentWatermark();
    }

    @Override
    public void registerProcessingTimeTimer(N namespace, long time) {
        currentInternalTimerService.registerProcessingTimeTimer(namespace, time);
    }

    @Override
    public void deleteProcessingTimeTimer(N namespace, long time) {
        currentInternalTimerService.deleteProcessingTimeTimer(namespace, time);
    }

    @Override
    public void registerEventTimeTimer(N namespace, long time) {
        currentInternalTimerService.registerEventTimeTimer(namespace, time);
    }

    @Override
    public void deleteEventTimeTimer(N namespace, long time) {
        currentInternalTimerService.deleteEventTimeTimer(namespace, time);
    }

    @Override
    public void forEachEventTimeTimer(BiConsumerWithException<N, Long, Exception> consumer)
            throws Exception {
        currentInternalTimerService.forEachEventTimeTimer(consumer);
    }

    @Override
    public void forEachProcessingTimeTimer(BiConsumerWithException<N, Long, Exception> consumer)
            throws Exception {
        currentInternalTimerService.forEachProcessingTimeTimer(consumer);
    }

    public void advanceWatermark(long timestamp) throws Exception {
        if (currentInternalTimerService == batchExecutionInternalTimeService) {
            batchExecutionInternalTimeService.setBacklogWatermark(timestamp);
        }
        internalTimerService.advanceWatermark(timestamp);
    }

    public void setBacklog(boolean backlog) throws Exception {
        if (currentInternalTimerService == batchExecutionInternalTimeService && !backlog) {
            batchExecutionInternalTimeService.setCurrentKey(null);
            currentInternalTimerService = internalTimerService;
            return;
        }

        if (currentInternalTimerService == internalTimerService && backlog) {
            currentInternalTimerService = batchExecutionInternalTimeService;
            return;
        }
    }

    public void setCurrentKey(K newKey) throws Exception {
        if (currentInternalTimerService != batchExecutionInternalTimeService) {
            return;
        }
        batchExecutionInternalTimeService.setCurrentKey(newKey);
    }
}
