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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.WatermarkAlignedSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.PausableSplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.connector.base.source.reader.splitreader.PausableSplitReader.SplitState;

/**
 * This class extends {@link SplitFetcherManager} to support pausing and resuming reading splits.
 * This class work with {@link WatermarkAlignedSourceReaderBase}.
 */
public abstract class WatermarkAlignedSplitFetcherManager<E, SplitT extends SourceSplit>
        extends SplitFetcherManager<E, SplitT> {

    private final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue;
    private final Supplier<PausableSplitReader<E, SplitT>> splitReaderFactory;

    /**
     * Create a split fetcher manager.
     *
     * @param elementsQueue the queue that split readers will put elements into.
     * @param splitReaderFactory a supplier that could be used to create split readers.
     */
    public WatermarkAlignedSplitFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<PausableSplitReader<E, SplitT>> splitReaderFactory) {
        super(elementsQueue, splitReaderFactory::get);
        this.elementsQueue = elementsQueue;
        this.splitReaderFactory = splitReaderFactory;
    }

    @Override
    protected synchronized PausableSplitFetcher<E, SplitT> createSplitFetcher() {
        if (closed) {
            throw new IllegalStateException("The split fetcher manager has closed.");
        }
        // Create SplitReader.
        PausableSplitReader<E, SplitT> splitReader = splitReaderFactory.get();

        int fetcherId = fetcherIdGenerator.getAndIncrement();
        PausableSplitFetcher<E, SplitT> splitFetcher =
                new PausableSplitFetcher<>(
                        fetcherId,
                        elementsQueue,
                        splitReader,
                        errorHandler,
                        () -> {
                            fetchers.remove(fetcherId);
                            // We need this to synchronize status of fetchers to concurrent partners
                            // as
                            // ConcurrentHashMap's aggregate status methods including size, isEmpty,
                            // and
                            // containsValue are not designed for program control.
                            elementsQueue.notifyAvailable();
                        });
        fetchers.put(fetcherId, splitFetcher);
        return splitFetcher;
    }

    public void updateSplitsState(Map<String, SplitState> desiredState) {
        for (SplitFetcher<E, SplitT> splitFetcher : fetchers.values()) {
            ((PausableSplitFetcher<E, SplitT>) splitFetcher).updateSplitStates(desiredState);
        }
    }

    public Collection<String> getRunningSplits() {
        Collection<String> runningSplits = new LinkedList<>();
        fetchers.forEach(
                (fetcherId, fetcher) -> {
                    final Map<String, SplitState> splitsState =
                            ((PausableSplitFetcher<E, SplitT>) fetcher).getSplitsState();
                    splitsState.forEach(
                            (id, state) -> {
                                runningSplits.add(id);
                            });
                });
        return runningSplits;
    }
}
