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

package org.apache.flink.connector.base.source.splitenumerator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.base.source.event.GlobalWatermarkEvent;
import org.apache.flink.connector.base.source.event.SourceReaderWatermarkEvent;
import org.apache.flink.connector.base.source.reader.WatermarkAlignedSourceReaderBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The WatermarkAlignedSplitEnumerator will handle {@link SourceReaderWatermarkEvent} sent by the
 * {@link WatermarkAlignedSourceReaderBase}. It will send the {@link GlobalWatermarkEvent} to each
 * SourceReader periodically.
 */
public abstract class WatermarkAlignedSplitEnumerator<SplitT extends SourceSplit, CheckpointT>
        implements SplitEnumerator<SplitT, CheckpointT> {

    private static final Logger LOG =
            LoggerFactory.getLogger(WatermarkAlignedSplitEnumerator.class);
    public final SplitEnumeratorContext<SplitT> context;
    private final Map<Integer, Long> sourceReaderWatermarks = new HashMap<>();
    private final Set<Integer> subtaskIds = new HashSet<>();
    private final long period;

    public WatermarkAlignedSplitEnumerator(SplitEnumeratorContext<SplitT> context, long period) {
        this.context = context;
        this.period = period;
    }

    @Override
    public void start() {
        if (period > 0) {
            context.callAsync(this::getGlobalWatermark, this::sendGlobalWatermark, 0, period);
        }
    }

    @Override
    public void addReader(int subtaskId) {
        subtaskIds.add(subtaskId);
    }

    private void sendGlobalWatermark(Long globalWatermark, Throwable throwable) {
        if (sourceReaderWatermarks.isEmpty()) {
            return;
        }
        for (Integer subtaskId : subtaskIds) {
            context.sendEventToSourceReader(subtaskId, new GlobalWatermarkEvent(globalWatermark));
        }
    }

    private Long getGlobalWatermark() {
        if (sourceReaderWatermarks.isEmpty()) {
            return Long.MIN_VALUE;
        }
        return Collections.min(sourceReaderWatermarks.values());
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (period > 0 && sourceEvent instanceof SourceReaderWatermarkEvent) {
            final Long watermark = ((SourceReaderWatermarkEvent) sourceEvent).getWatermark();
            if (watermark == null) {
                sourceReaderWatermarks.remove(subtaskId);
                return;
            }
            sourceReaderWatermarks.put(subtaskId, watermark);
        } else {
            LOG.warn("Receive a unknown event {}", sourceEvent);
        }
    }
}
