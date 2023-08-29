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

package org.apache.flink.streaming.examples.backlog;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND_CACHE_SIZE;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL_DURING_BACKLOG;

public class CoProcessBacklogBenchmark {
    public static void main(String[] args) throws Exception {

        int numKeys = Integer.parseInt(args[0]);
        long numBacklogRecords = Long.parseLong(args[1]);
        long numRealTimeRecords = Long.parseLong(args[2]);
        boolean enableBacklog = Boolean.parseBoolean(args[3]);
        boolean useBatch = Boolean.parseBoolean(args[4]);
        //        int numRuns = Integer.parseInt(args[5]);

        final Configuration config = new Configuration();
        config.set(STATE_BACKEND, "rocksdb");
        if (useBatch) {
            config.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        } else if (enableBacklog) {
            config.set(CHECKPOINTING_INTERVAL_DURING_BACKLOG, Duration.ZERO);
            config.set(STATE_BACKEND_CACHE_SIZE, 1);
        }

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);

        final DataGeneratorSource<Tuple3<Integer, Long, Long>> historicalData =
                new DataGeneratorSource<>(
                        new GeneratorFunction<Long, Tuple3<Integer, Long, Long>>() {
                            @Override
                            public Tuple3<Integer, Long, Long> map(Long value) throws Exception {
                                return new Tuple3<>(value.intValue() % numKeys, value, 1L);
                            }
                        },
                        numBacklogRecords,
                        Types.TUPLE(Types.INT, Types.LONG));

        final DataGeneratorSource<Tuple3<Integer, Long, Long>> realTimeData =
                new DataGeneratorSource<>(
                        new GeneratorFunction<Long, Tuple3<Integer, Long, Long>>() {

                            @Override
                            public Tuple3<Integer, Long, Long> map(Long value) throws Exception {
                                return new Tuple3<>(
                                        value.intValue() % numKeys, value + numBacklogRecords, 1L);
                            }
                        },
                        numRealTimeRecords,
                        Types.TUPLE(Types.INT, Types.LONG));

        final HybridSource<Tuple3<Integer, Long, Long>> source1 =
                HybridSource.builder(historicalData).addSource(realTimeData).build();

        final HybridSource<Tuple3<Integer, Long, Long>> source2 =
                HybridSource.builder(historicalData).addSource(realTimeData).build();

        final SingleOutputStreamOperator<Tuple3<Integer, Long, Long>> processed =
                env.fromSource(
                                source1,
                                WatermarkStrategy
                                        .<Tuple3<Integer, Long, Long>>forMonotonousTimestamps()
                                        .withTimestampAssigner((event, ts) -> event.f1),
                                "source1")
                        .returns(Types.TUPLE(Types.INT, Types.LONG, Types.LONG))
                        .connect(
                                env.fromSource(
                                                source2,
                                                WatermarkStrategy
                                                        .<Tuple3<Integer, Long, Long>>
                                                                forMonotonousTimestamps()
                                                        .withTimestampAssigner(
                                                                (event, ts) -> event.f1),
                                                "Source2")
                                        .returns(Types.TUPLE(Types.INT, Types.LONG, Types.LONG)))
                        .keyBy(
                                (KeySelector<Tuple3<Integer, Long, Long>, Integer>)
                                        value -> value.f0,
                                (KeySelector<Tuple3<Integer, Long, Long>, Integer>)
                                        value -> value.f0)
                        .process(new MyKeyedCoProcessFunction());

        processed
                .getTransformation()
                .declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 128);

        processed.print();
        //        processed.addSink(new DiscardingSink<>());

        env.execute();
    }

    public static class MyKeyedCoProcessFunction
            extends KeyedCoProcessFunction<
                    Integer,
                    Tuple3<Integer, Long, Long>,
                    Tuple3<Integer, Long, Long>,
                    Tuple3<Integer, Long, Long>> {

        private ValueState<Tuple3<Integer, Long, Long>> leftValue;
        private ValueState<Tuple3<Integer, Long, Long>> rightValue;

        @Override
        public void open(Configuration parameters) throws Exception {
            leftValue =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "left_value",
                                            Types.TUPLE(Types.INT, Types.LONG, Types.LONG)));

            rightValue =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "right_value",
                                            Types.TUPLE(Types.INT, Types.LONG, Types.LONG)));
        }

        @Override
        public void processElement1(
                Tuple3<Integer, Long, Long> value,
                KeyedCoProcessFunction<
                                        Integer,
                                        Tuple3<Integer, Long, Long>,
                                        Tuple3<Integer, Long, Long>,
                                        Tuple3<Integer, Long, Long>>
                                .Context
                        ctx,
                Collector<Tuple3<Integer, Long, Long>> out)
                throws Exception {
            final Tuple3<Integer, Long, Long> right = rightValue.value();
            if (right != null) {
                out.collect(new Tuple3<>(value.f0, value.f1, value.f2 + right.f2));
            }

            leftValue.update(value);
        }

        @Override
        public void processElement2(
                Tuple3<Integer, Long, Long> value,
                KeyedCoProcessFunction<
                                        Integer,
                                        Tuple3<Integer, Long, Long>,
                                        Tuple3<Integer, Long, Long>,
                                        Tuple3<Integer, Long, Long>>
                                .Context
                        ctx,
                Collector<Tuple3<Integer, Long, Long>> out)
                throws Exception {
            final Tuple3<Integer, Long, Long> left = leftValue.value();
            if (left != null) {
                out.collect(new Tuple3<>(value.f0, value.f1, value.f2 + left.f2));
            }
            rightValue.update(value);
        }
    }
}
