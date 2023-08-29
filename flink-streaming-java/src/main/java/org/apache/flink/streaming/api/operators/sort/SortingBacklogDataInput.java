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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.RecordAttributes;
import org.apache.flink.runtime.event.RecordAttributesBuilder;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.PushSorter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/** SortingBacklogDataInput. */
@Internal
public class SortingBacklogDataInput<T, K> implements StreamTaskInput<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SortingBacklogDataInput.class);

    private final StreamTaskInput<T> wrappedInput;
    private final PushSorter<Tuple2<byte[], StreamRecord<T>>> sorter;
    private final KeySelector<T, K> keySelector;
    private final TypeSerializer<K> keySerializer;
    private final DataOutputSerializer dataOutputSerializer;
    private final SortingDataOutput sortingDataOutput;
    private final StreamTask.CanEmitBatchOfRecordsChecker canEmitBatchOfRecords;
    private MutableObjectIterator<Tuple2<byte[], StreamRecord<T>>> sortedInput = null;
    private long watermarkSeen = Long.MIN_VALUE;

    private volatile OperatingMode mode = OperatingMode.PROCESSING_REALTIME;

    private enum OperatingMode {
        PROCESSING_REALTIME,
        SORTING_BACKLOG,
        FLUSHING_BACKLOG
    }

    public SortingBacklogDataInput(
            StreamTaskInput<T> wrappedInput,
            TypeSerializer<T> typeSerializer,
            TypeSerializer<K> keySerializer,
            KeySelector<T, K> keySelector,
            MemoryManager memoryManager,
            IOManager ioManager,
            boolean objectReuse,
            double managedMemoryFraction,
            Configuration taskManagerConfiguration,
            TaskInvokable containingTask,
            ExecutionConfig executionConfig,
            StreamTask.CanEmitBatchOfRecordsChecker canEmitBatchOfRecords) {
        try {
            this.canEmitBatchOfRecords = canEmitBatchOfRecords;
            this.sortingDataOutput = new SortingDataOutput();
            this.keySelector = keySelector;
            this.keySerializer = keySerializer;
            int keyLength = keySerializer.getLength();
            final TypeComparator<Tuple2<byte[], StreamRecord<T>>> comparator;
            if (keyLength > 0) {
                this.dataOutputSerializer = new DataOutputSerializer(keyLength);
                comparator = new FixedLengthByteKeyComparator<>(keyLength);
            } else {
                this.dataOutputSerializer = new DataOutputSerializer(64);
                comparator = new VariableLengthByteKeyComparator<>();
            }
            KeyAndValueSerializer<T> keyAndValueSerializer =
                    new KeyAndValueSerializer<>(typeSerializer, keyLength);
            this.wrappedInput = wrappedInput;
            this.sorter =
                    ExternalSorter.newBuilder(
                                    memoryManager,
                                    containingTask,
                                    keyAndValueSerializer,
                                    comparator,
                                    executionConfig)
                            .memoryFraction(managedMemoryFraction)
                            .enableSpilling(
                                    ioManager,
                                    taskManagerConfiguration.get(
                                            AlgorithmOptions.SORT_SPILLING_THRESHOLD))
                            .maxNumFileHandles(
                                    taskManagerConfiguration.get(AlgorithmOptions.SPILLING_MAX_FAN))
                            .objectReuse(objectReuse)
                            .largeRecords(
                                    taskManagerConfiguration.get(
                                            AlgorithmOptions.USE_LARGE_RECORDS_HANDLER))
                            .build();
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getInputIndex() {
        return wrappedInput.getInputIndex();
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        if (mode != OperatingMode.PROCESSING_REALTIME) {
            throw new UnsupportedOperationException(
                    "Checkpoints are not supported during backlog.");
        }
        return wrappedInput.prepareSnapshot(channelStateWriter, checkpointId);
    }

    @Override
    public void close() throws IOException {
        IOException ex = null;
        try {
            wrappedInput.close();
        } catch (IOException e) {
            ex = ExceptionUtils.firstOrSuppressed(e, ex);
        }

        try {
            sorter.close();
        } catch (IOException e) {
            ex = ExceptionUtils.firstOrSuppressed(e, ex);
        }

        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {
        LOG.debug("Emit next, current mode: {}", mode);
        if (sortingDataOutput.innerOutput != output) {
            sortingDataOutput.innerOutput = output;
        }

        if (mode == OperatingMode.PROCESSING_REALTIME) {
            return wrappedInput.emitNext(sortingDataOutput);
        }

        if (mode == OperatingMode.SORTING_BACKLOG) {
            return wrappedInput.emitNext(sortingDataOutput);
        }

        if (mode == OperatingMode.FLUSHING_BACKLOG) {
            while (true) {
                final DataInputStatus status = emitNextSortedRecord(output);
                if (status == DataInputStatus.MORE_AVAILABLE
                        && canEmitBatchOfRecords.check()
                        && mode == OperatingMode.FLUSHING_BACKLOG) {
                    continue;
                }
                return status;
            }
        }

        // Should never reach here
        throw new RuntimeException(String.format("Unknown OperatingMode %s", mode));
    }

    @Nonnull
    private DataInputStatus emitNextSortedRecord(DataOutput<T> output) throws Exception {
        Tuple2<byte[], StreamRecord<T>> next = sortedInput.next();
        if (next != null) {
            output.emitRecord(next.f1);
        } else {
            // Finished flushing
            mode = OperatingMode.PROCESSING_REALTIME;

            // Send backlog=false downstream
            output.emitRecordAttributes(
                    new RecordAttributesBuilder(Collections.emptyList()).setBacklog(false).build());

            if (watermarkSeen > Long.MIN_VALUE) {
                output.emitWatermark(new Watermark(watermarkSeen));
            }
        }
        return DataInputStatus.MORE_AVAILABLE;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (mode == OperatingMode.FLUSHING_BACKLOG) {
            return AvailabilityProvider.AVAILABLE;
        } else {
            return wrappedInput.getAvailableFuture();
        }
    }

    private class SortingDataOutput implements DataOutput<T> {

        private DataOutput<T> innerOutput;

        @Override
        public void emitRecord(StreamRecord<T> streamRecord) throws Exception {
            LOG.debug("Emit record {}", streamRecord.getValue());
            if (mode == OperatingMode.PROCESSING_REALTIME) {
                innerOutput.emitRecord(streamRecord);
                return;
            }

            if (mode == OperatingMode.SORTING_BACKLOG) {
                K key = keySelector.getKey(streamRecord.getValue());

                keySerializer.serialize(key, dataOutputSerializer);
                byte[] serializedKey = dataOutputSerializer.getCopyOfBuffer();
                dataOutputSerializer.clear();

                sorter.writeRecord(Tuple2.of(serializedKey, streamRecord));
                return;
            }

            if (mode == OperatingMode.FLUSHING_BACKLOG) {
                throw new RuntimeException("Unexpected StreamRecord during FLUSHING_BACKLOG.");
            }
        }

        @Override
        public void emitWatermark(Watermark watermark) throws Exception {
            if (mode == OperatingMode.PROCESSING_REALTIME) {
                innerOutput.emitWatermark(watermark);
            } else {
                watermarkSeen = Math.max(watermarkSeen, watermark.getTimestamp());
            }
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            if (mode == OperatingMode.PROCESSING_REALTIME) {
                innerOutput.emitWatermarkStatus(watermarkStatus);
            }

            // Ignore watermark status during backlog
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            if (mode == OperatingMode.PROCESSING_REALTIME) {
                innerOutput.emitLatencyMarker(latencyMarker);
            }

            // Ignore watermark status during backlog
        }

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) throws Exception {
            LOG.debug("Emit record attributes {}", recordAttributes);
            if (mode == OperatingMode.PROCESSING_REALTIME && recordAttributes.isBacklog()) {
                // switch to backlog
                mode = OperatingMode.SORTING_BACKLOG;
                innerOutput.emitRecordAttributes(recordAttributes);
                return;
            }

            if (mode == OperatingMode.SORTING_BACKLOG && !recordAttributes.isBacklog()) {
                // switching to realtime, we flush backlog first
                innerOutput.emitWatermark(new Watermark(watermarkSeen));
                sorter.finishReading();
                sortedInput = sorter.getIterator();
                mode = OperatingMode.FLUSHING_BACKLOG;
                return;
            }

            if (mode == OperatingMode.FLUSHING_BACKLOG && recordAttributes.isBacklog()) {
                throw new RuntimeException(
                        "Should not receive record attribute while flushing backlog.");
            }
        }
    }
}
