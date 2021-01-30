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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.PausableSplitFetcherTest;
import org.apache.flink.connector.base.source.reader.fetcher.WatermarkAlignedSingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.mocks.MockRecordEmitter;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.splitenumerator.WatermarkAlignedSplitEnumerator;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** IT test case for {@link WatermarkAlignedSourceReaderBase}. */
public class WatermarkAlignedSourceReaderBaseIT {

    @Test
    public void testWatermarkAlignedSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        WatermarkAlignmentTestSource source = new WatermarkAlignmentTestSource();
        env.fromSource(
                        source,
                        WatermarkStrategy.forGenerator((context) -> new TestWatermarkGenerator()),
                        "TestingSource")
                .addSink(
                        new RichSinkFunction<Integer>() {
                            @Override
                            public void open(Configuration parameters) throws Exception {}

                            @Override
                            public void invoke(Integer value, Context context) throws Exception {}
                        })
                .setParallelism(1);
        env.execute();
    }

    static class TestWatermarkGenerator implements WatermarkGenerator<Integer>, Serializable {
        Long lastEmittedWatermark = null;

        @Override
        public void onEvent(Integer event, long eventTimestamp, WatermarkOutput output) {
            output.emitWatermark(new Watermark(event));
            lastEmittedWatermark = Long.valueOf(event);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }

    static class WatermarkAlignmentTestSource
            implements Source<Integer, MockSourceSplit, List<MockSourceSplit>> {

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<Integer, MockSourceSplit> createReader(
                SourceReaderContext readerContext) {
            FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
                    new FutureCompletingBlockingQueue<>();
            Configuration config = new Configuration();
            config.setInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1);
            config.setLong(SourceReaderOptions.SOURCE_READER_CLOSE_TIMEOUT, 30000L);
            return new WatermarkAlignedSourceReaderBaseTest.MockWatermarkAlignedSourceReaderBase(
                    elementsQueue,
                    new WatermarkAlignedSingleThreadFetcherManager<>(
                            elementsQueue,
                            () -> new PausableSplitFetcherTest.PausableMockSplitReader(1, false)),
                    new MockRecordEmitter(),
                    config,
                    readerContext);
        }

        @Override
        public SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> createEnumerator(
                SplitEnumeratorContext<MockSourceSplit> enumContext) {
            return new MockSplitEnumerator(getSplits(), enumContext, 100);
        }

        private List<MockSourceSplit> getSplits() {
            List<MockSourceSplit> splits = new ArrayList<>();
            splits.add(getFastSplit());
            splits.add(getSlowSplit());
            return splits;
        }

        private MockSourceSplit getSlowSplit() {
            MockSourceSplit split = new MockSourceSplit(0, 0, 200);
            for (int i = 0; i < 200; i++) {
                split.addRecord(i);
            }
            return split;
        }

        private MockSourceSplit getFastSplit() {
            MockSourceSplit split = new MockSourceSplit(1, 1000, 1200);
            for (int i = 1000; i < 1200; i++) {
                split.addRecord(i);
            }
            return split;
        }

        @Override
        public SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> restoreEnumerator(
                SplitEnumeratorContext<MockSourceSplit> enumContext,
                List<MockSourceSplit> checkpoint) {
            return new MockSplitEnumerator(checkpoint, enumContext, 100);
        }

        @Override
        public SimpleVersionedSerializer<MockSourceSplit> getSplitSerializer() {
            return new MockSourceSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<List<MockSourceSplit>>
                getEnumeratorCheckpointSerializer() {
            return new SimpleVersionedSerializer<List<MockSourceSplit>>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                @Override
                public byte[] serialize(List<MockSourceSplit> obj) throws IOException {
                    return InstantiationUtil.serializeObject(obj.toArray());
                }

                @Override
                public List<MockSourceSplit> deserialize(int version, byte[] serialized)
                        throws IOException {
                    MockSourceSplit[] splitArray;
                    try {
                        splitArray =
                                InstantiationUtil.deserializeObject(
                                        serialized, getClass().getClassLoader());
                    } catch (ClassNotFoundException e) {
                        throw new IOException("Failed to deserialize the source split.");
                    }
                    return new ArrayList<>(Arrays.asList(splitArray));
                }
            };
        }
    }

    static class MockSplitEnumerator
            extends WatermarkAlignedSplitEnumerator<MockSourceSplit, List<MockSourceSplit>> {
        private final List<MockSourceSplit> splits;
        private final SplitEnumeratorContext<MockSourceSplit> context;

        public MockSplitEnumerator(
                List<MockSourceSplit> splits,
                SplitEnumeratorContext<MockSourceSplit> context,
                long period) {
            super(context, period);
            this.splits = splits;
            this.context = context;
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            // do nothing
        }

        @Override
        public void addSplitsBack(List<MockSourceSplit> splits, int subtaskId) {
            this.splits.addAll(splits);
        }

        @Override
        public void addReader(int subtaskId) {
            super.addReader(subtaskId);
            if (context.registeredReaders().size() == context.currentParallelism()) {
                int numReaders = context.registeredReaders().size();
                Map<Integer, List<MockSourceSplit>> assignment = new HashMap<>();
                for (int i = 0; i < splits.size(); i++) {
                    assignment
                            .computeIfAbsent(i % numReaders, t -> new ArrayList<>())
                            .add(splits.get(i));
                }
                context.assignSplits(new SplitsAssignment<>(assignment));
                splits.clear();
                for (int i = 0; i < numReaders; i++) {
                    context.signalNoMoreSplits(i);
                }
            }
        }

        @Override
        public List<MockSourceSplit> snapshotState() throws Exception {
            return splits;
        }

        @Override
        public void close() throws IOException {}
    }
}
