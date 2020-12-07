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
import org.apache.flink.connector.base.source.reader.splitreader.PausableSplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.flink.connector.base.source.reader.splitreader.PausableSplitReader.SplitState;

/** SplitFetcher that can pause or resume reading from splits. */
public class PausableSplitFetcher<E, SplitT extends SourceSplit> extends SplitFetcher<E, SplitT> {
    private final PausableSplitReader<E, SplitT> splitReader;

    PausableSplitFetcher(
            int id,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            PausableSplitReader<E, SplitT> splitReader,
            Consumer<Throwable> errorHandler,
            Runnable shutdownHook) {
        super(id, elementsQueue, splitReader, errorHandler, shutdownHook);
        this.splitReader = splitReader;
    }

    public void updateSplitStates(Map<String, SplitState> desiredStates) {
        enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() {
                        Map<String, SplitState> splitsState = splitReader.getSplitState();
                        Set<String> splitToPause = new HashSet<>();
                        Set<String> splitToResume = new HashSet<>();
                        desiredStates.forEach(
                                (splitId, desiredState) -> {
                                    SplitState currentState = splitsState.get(splitId);
                                    if (currentState == null) {
                                        return;
                                    }
                                    if (currentState == desiredState) {
                                        // state is not changed do nothing
                                        return;
                                    }
                                    if (currentState == SplitState.RUN) {
                                        // split should pause
                                        splitToPause.add(splitId);
                                    } else if (currentState == SplitState.PAUSE) {
                                        // split should resume
                                        splitToResume.add(splitId);
                                    }
                                });
                        if (splitToPause.size() > 0) {
                            splitReader.pause(splitToPause);
                        }
                        if (splitToResume.size() > 0) {
                            splitReader.resume(splitToResume);
                        }
                        return true;
                    }

                    @Override
                    public void wakeUp() {
                        // do nothing
                    }
                });
        wakeUp(true);
    }

    public Map<String, SplitState> getSplitsState() {
        return splitReader.getSplitState();
    }
}
