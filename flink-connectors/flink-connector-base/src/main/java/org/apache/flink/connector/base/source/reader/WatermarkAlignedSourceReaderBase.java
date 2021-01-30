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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.event.GlobalWatermarkEvent;
import org.apache.flink.connector.base.source.event.SourceReaderWatermarkEvent;
import org.apache.flink.connector.base.source.reader.fetcher.WatermarkAlignedSplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.PausableSplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An WatermarkAlignedSourceReaderBase extends {@link SourceReaderBase} to support watermark
 * alignment. It reports the watermark of all the running splits it has periodically. It can act on
 * the {@link GlobalWatermarkEvent} to pause reading split when the watermark of the split is
 * greater than the global watermark by some threshold or resume reading when its watermark goes
 * below the threshold.
 */
public abstract class WatermarkAlignedSourceReaderBase<
                E, T, SplitT extends SourceSplit, SplitStateT>
        extends SourceReaderBase<E, T, SplitT, SplitStateT> {

    private static final Logger LOG =
            LoggerFactory.getLogger(WatermarkAlignedSourceReaderBase.class);
    public final WatermarkAlignedSplitFetcherManager<E, SplitT> splitFetcherManager;
    private final long threshold;
    private final long reportPeriod;
    private final ScheduledExecutorService executor;

    public WatermarkAlignedSourceReaderBase(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            WatermarkAlignedSplitFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            long threshold,
            long reportPeriod) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
        this.splitFetcherManager = splitFetcherManager;
        this.threshold = threshold;
        this.reportPeriod = reportPeriod;
        this.executor =
                Executors.newSingleThreadScheduledExecutor(
                        r ->
                                new Thread(
                                        r,
                                        context.getTaskName()
                                                + "-"
                                                + context.getIndexOfSubtask()
                                                + "-SplitStatusReportThread"));
    }

    @Override
    public void start() {
        super.start();
        if (threshold > 0 && reportPeriod > 0) {
            executor.scheduleAtFixedRate(
                    () -> {
                        try {
                            final SourceReaderWatermarkEvent sourceEvent =
                                    new SourceReaderWatermarkEvent(getSourceReaderWatermark());
                            context.sendSourceEventToCoordinator(sourceEvent);
                        } catch (Throwable e) {
                            LOG.error("Error on sending split status", e);
                        }
                    },
                    0,
                    reportPeriod,
                    TimeUnit.MILLISECONDS);
        }
    }

    private Long getSourceReaderWatermark() {
        final Collection<Long> splitWatermarks = getAllSplitWatermarks().values();
        if (splitWatermarks.isEmpty()) {
            return null;
        }
        return Collections.min(splitWatermarks);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (threshold > 0 && reportPeriod > 0 && sourceEvent instanceof GlobalWatermarkEvent) {
            GlobalWatermarkEvent globalWatermarkEvent = (GlobalWatermarkEvent) sourceEvent;
            handleGlobalWatermarkEvent(globalWatermarkEvent);
            return;
        }
        super.handleSourceEvents(sourceEvent);
    }

    private void handleGlobalWatermarkEvent(GlobalWatermarkEvent globalWatermarkEvent) {
        Long globalWatermark = globalWatermarkEvent.getGlobalWatermark();
        Map<String, PausableSplitReader.SplitState> desiredState = new HashMap<>();
        splitStates.forEach(
                (splitId, splitContext) -> {
                    if (splitContext.getSourceOutput().getCurrentWatermark()
                            > globalWatermark + threshold) {
                        desiredState.put(splitId, PausableSplitReader.SplitState.PAUSE);
                    } else {
                        desiredState.put(splitId, PausableSplitReader.SplitState.RUN);
                    }
                });
        splitFetcherManager.updateSplitsState(desiredState);
    }

    Map<String, Long> getAllSplitWatermarks() {
        Map<String, Long> splitWatermarks = new HashMap<>();
        for (String splitId : splitFetcherManager.getRunningSplits()) {
            final SplitContext<T, SplitStateT> splitContext = splitStates.get(splitId);
            final SourceOutput<T> sourceOutput = splitContext.getSourceOutput();
            if (sourceOutput == null || sourceOutput.isIdle()) {
                continue;
            }
            long watermark = sourceOutput.getCurrentWatermark();
            splitWatermarks.put(splitId, watermark);
        }
        return splitWatermarks;
    }
}
