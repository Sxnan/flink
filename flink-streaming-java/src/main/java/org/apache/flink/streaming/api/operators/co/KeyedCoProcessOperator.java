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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.RuntimePairComparatorFactory;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.event.RecordAttributes;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.NonReusingSortMergeCoGroupIterator;
import org.apache.flink.runtime.operators.sort.PushSorter;
import org.apache.flink.runtime.operators.util.CoGroupTaskIterator;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.sort.FixedLengthByteKeyComparator;
import org.apache.flink.streaming.api.operators.sort.KeyAndValueSerializer;
import org.apache.flink.streaming.api.operators.sort.VariableLengthByteKeyComparator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing keyed {@link
 * KeyedCoProcessFunction KeyedCoProcessFunction}.
 */
@Internal
public class KeyedCoProcessOperator<K, IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<OUT, KeyedCoProcessFunction<K, IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT>, Triggerable<K, VoidNamespace> {

    private static final long serialVersionUID = 1L;

    private transient TimestampedCollector<OUT> collector;

    private transient ContextImpl<K, IN1, IN2, OUT> context;

    private transient OnTimerContextImpl<K, IN1, IN2, OUT> onTimerContext;

    private boolean backlog1 = false;

    private boolean backlog2 = false;

    private KeySelector<IN1, K> keySelectorA;
    private KeySelector<IN2, K> keySelectorB;
    private TypeSerializer<Object> keySerializer;
    private KeyAndValueSerializer<IN1> keyAndValueSerializerA;
    private KeyAndValueSerializer<IN2> keyAndValueSerializerB;
    private DataOutputSerializer dataOutputSerializer;
    private TypeComparator<Tuple2<byte[], StreamRecord<IN1>>> comparatorA;
    private TypeComparator<Tuple2<byte[], StreamRecord<IN2>>> comparatorB;
    private PushSorter<Tuple2<byte[], StreamRecord<IN1>>> sorterA;
    private PushSorter<Tuple2<byte[], StreamRecord<IN2>>> sorterB;
    private long lastWatermarkTimestamp = Long.MIN_VALUE;
    private boolean disableInternalSort;

    public KeyedCoProcessOperator(KeyedCoProcessFunction<K, IN1, IN2, OUT> keyedCoProcessFunction) {
        super(keyedCoProcessFunction);
    }

    @Override
    public void open() throws Exception {
        super.open();
        collector = new TimestampedCollector<>(output);

        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

        TimerService timerService = new SimpleTimerService(internalTimerService);

        context = new ContextImpl<>(userFunction, timerService);
        onTimerContext = new OnTimerContextImpl<>(userFunction, timerService);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);

        disableInternalSort =
                !config.isCheckpointIntervalDuringBacklogSpecified()
                        || config.getCheckpointIntervalDuringBacklog().toMillis()
                                != DISABLED_CHECKPOINT_INTERVAL;

        if (disableInternalSort) {
            return;
        }

        ClassLoader userCodeClassLoader = containingTask.getUserCodeClassLoader();
        MemoryManager memoryManager = containingTask.getEnvironment().getMemoryManager();
        IOManager ioManager = containingTask.getEnvironment().getIOManager();

        keySelectorA = (KeySelector<IN1, K>) config.getStatePartitioner(0, userCodeClassLoader);
        keySelectorB = (KeySelector<IN2, K>) config.getStatePartitioner(1, userCodeClassLoader);
        keySerializer = config.getStateKeySerializer(userCodeClassLoader);
        int keyLength = keySerializer.getLength();

        TypeSerializer<IN1> typeSerializerA = config.getTypeSerializerIn(0, userCodeClassLoader);
        TypeSerializer<IN2> typeSerializerB = config.getTypeSerializerIn(1, userCodeClassLoader);
        keyAndValueSerializerA = new KeyAndValueSerializer<>(typeSerializerA, keyLength);
        keyAndValueSerializerB = new KeyAndValueSerializer<>(typeSerializerB, keyLength);

        if (keyLength > 0) {
            dataOutputSerializer = new DataOutputSerializer(keyLength);
            comparatorA = new FixedLengthByteKeyComparator<>(keyLength);
            comparatorB = new FixedLengthByteKeyComparator<>(keyLength);
        } else {
            dataOutputSerializer = new DataOutputSerializer(64);
            comparatorA = new VariableLengthByteKeyComparator<>();
            comparatorB = new VariableLengthByteKeyComparator<>();
        }

        ExecutionConfig executionConfig = containingTask.getEnvironment().getExecutionConfig();

        // TODO: calculate managedMemoryFraction properly.
        double managedMemoryFraction =
                config.getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.OPERATOR,
                                containingTask.getEnvironment().getTaskConfiguration(),
                                userCodeClassLoader)
                        / 2;

        Configuration jobConfiguration = containingTask.getEnvironment().getJobConfiguration();

        try {
            sorterA =
                    ExternalSorter.newBuilder(
                                    memoryManager,
                                    containingTask,
                                    keyAndValueSerializerA,
                                    comparatorA,
                                    executionConfig)
                            .memoryFraction(managedMemoryFraction)
                            .enableSpilling(
                                    ioManager,
                                    jobConfiguration.get(AlgorithmOptions.SORT_SPILLING_THRESHOLD))
                            .maxNumFileHandles(
                                    jobConfiguration.get(AlgorithmOptions.SPILLING_MAX_FAN) / 2)
                            .objectReuse(executionConfig.isObjectReuseEnabled())
                            .largeRecords(
                                    jobConfiguration.get(
                                            AlgorithmOptions.USE_LARGE_RECORDS_HANDLER))
                            .build();
            sorterB =
                    ExternalSorter.newBuilder(
                                    memoryManager,
                                    containingTask,
                                    keyAndValueSerializerB,
                                    comparatorB,
                                    executionConfig)
                            .memoryFraction(managedMemoryFraction)
                            .enableSpilling(
                                    ioManager,
                                    jobConfiguration.get(AlgorithmOptions.SORT_SPILLING_THRESHOLD))
                            .maxNumFileHandles(
                                    jobConfiguration.get(AlgorithmOptions.SPILLING_MAX_FAN) / 2)
                            .objectReuse(executionConfig.isObjectReuseEnabled())
                            .largeRecords(
                                    jobConfiguration.get(
                                            AlgorithmOptions.USE_LARGE_RECORDS_HANDLER))
                            .build();
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        if (!isBacklog() || disableInternalSort) {
            collector.setTimestamp(element);
            context.element = element;
            userFunction.processElement1(element.getValue(), context, collector);
            context.element = null;
        } else {
            K key = keySelectorA.getKey(element.getValue());
            keySerializer.serialize(key, dataOutputSerializer);
            byte[] serializedKey = dataOutputSerializer.getCopyOfBuffer();
            dataOutputSerializer.clear();
            sorterA.writeRecord(Tuple2.of(serializedKey, element));
        }
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        if (!isBacklog() || disableInternalSort) {
            collector.setTimestamp(element);
            context.element = element;
            userFunction.processElement2(element.getValue(), context, collector);
            context.element = null;
        } else {
            K key = keySelectorB.getKey(element.getValue());
            keySerializer.serialize(key, dataOutputSerializer);
            byte[] serializedKey = dataOutputSerializer.getCopyOfBuffer();
            dataOutputSerializer.clear();
            sorterB.writeRecord(Tuple2.of(serializedKey, element));
        }
    }

    @Override
    public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        collector.setAbsoluteTimestamp(timer.getTimestamp());
        onTimerContext.timeDomain = TimeDomain.EVENT_TIME;
        onTimerContext.timer = timer;
        userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
        onTimerContext.timeDomain = null;
        onTimerContext.timer = null;
    }

    @Override
    public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        collector.eraseTimestamp();
        onTimerContext.timeDomain = TimeDomain.PROCESSING_TIME;
        onTimerContext.timer = timer;
        userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
        onTimerContext.timeDomain = null;
        onTimerContext.timer = null;
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        if (lastWatermarkTimestamp > watermark.getTimestamp()) {
            throw new RuntimeException("Invalid watermark");
        }
        lastWatermarkTimestamp = watermark.getTimestamp();
    }

    @Override
    public void processRecordAttributes1(RecordAttributes recordAttributes) throws Exception {
        final boolean preBacklog = isBacklog();
        backlog1 = recordAttributes.isBacklog();
        if (preBacklog && !isBacklog() && !disableInternalSort) {
            flushSortedInput();
        }
        super.processRecordAttributes1(recordAttributes);
    }

    @Override
    public void processRecordAttributes2(RecordAttributes recordAttributes) throws Exception {
        final boolean preBacklog = isBacklog();
        backlog2 = recordAttributes.isBacklog();
        if (preBacklog && !isBacklog() && !disableInternalSort) {
            flushSortedInput();
        }
        super.processRecordAttributes2(recordAttributes);
    }

    private void flushSortedInput() throws Exception {
        sorterA.finishReading();
        sorterB.finishReading();
        MutableObjectIterator<Tuple2<byte[], StreamRecord<IN1>>> iteratorA = sorterA.getIterator();
        MutableObjectIterator<Tuple2<byte[], StreamRecord<IN2>>> iteratorB = sorterB.getIterator();

        // TODO: enable re-use
        TypePairComparator<Tuple2<byte[], StreamRecord<IN1>>, Tuple2<byte[], StreamRecord<IN2>>>
                pairComparator =
                        (new RuntimePairComparatorFactory<
                                        Tuple2<byte[], StreamRecord<IN1>>,
                                        Tuple2<byte[], StreamRecord<IN2>>>())
                                .createComparator12(comparatorA, comparatorB);

        CoGroupTaskIterator<Tuple2<byte[], StreamRecord<IN1>>, Tuple2<byte[], StreamRecord<IN2>>>
                coGroupIterator =
                        new NonReusingSortMergeCoGroupIterator<>(
                                iteratorA,
                                iteratorB,
                                keyAndValueSerializerA,
                                comparatorA,
                                keyAndValueSerializerB,
                                comparatorB,
                                pairComparator);

        coGroupIterator.open();

        while (coGroupIterator.next()) {
            for (Tuple2<byte[], StreamRecord<IN1>> v1 : coGroupIterator.getValues1()) {
                userFunction.processElement1(v1.f1.getValue(), context, collector);
            }

            for (Tuple2<byte[], StreamRecord<IN2>> v2 : coGroupIterator.getValues2()) {
                userFunction.processElement2(v2.f1.getValue(), context, collector);
            }
        }

        coGroupIterator.close();

        Watermark watermark = new Watermark(lastWatermarkTimestamp);
        if (getTimeServiceManager().isPresent()) {
            getTimeServiceManager().get().advanceWatermark(watermark);
        }
        output.emitWatermark(watermark);
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder().setInternalSorterSupported(true).build();
    }

    private boolean isBacklog() {
        return backlog1 || backlog2;
    }

    protected TimestampedCollector<OUT> getCollector() {
        return collector;
    }

    private class ContextImpl<K, IN1, IN2, OUT>
            extends KeyedCoProcessFunction<K, IN1, IN2, OUT>.Context {

        private final TimerService timerService;

        private StreamRecord<?> element;

        ContextImpl(KeyedCoProcessFunction<K, IN1, IN2, OUT> function, TimerService timerService) {
            function.super();
            this.timerService = checkNotNull(timerService);
        }

        @Override
        public Long timestamp() {
            checkState(element != null);

            if (element.hasTimestamp()) {
                return element.getTimestamp();
            } else {
                return null;
            }
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }

            output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
        }

        @Override
        public K getCurrentKey() {
            return (K) KeyedCoProcessOperator.this.getCurrentKey();
        }
    }

    private class OnTimerContextImpl<K, IN1, IN2, OUT>
            extends KeyedCoProcessFunction<K, IN1, IN2, OUT>.OnTimerContext {

        private final TimerService timerService;

        private TimeDomain timeDomain;

        private InternalTimer<K, VoidNamespace> timer;

        OnTimerContextImpl(
                KeyedCoProcessFunction<K, IN1, IN2, OUT> function, TimerService timerService) {
            function.super();
            this.timerService = checkNotNull(timerService);
        }

        @Override
        public Long timestamp() {
            checkState(timer != null);
            return timer.getTimestamp();
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }

            output.collect(outputTag, new StreamRecord<>(value, timer.getTimestamp()));
        }

        @Override
        public TimeDomain timeDomain() {
            checkState(timeDomain != null);
            return timeDomain;
        }

        @Override
        public K getCurrentKey() {
            return timer.getKey();
        }
    }
}
