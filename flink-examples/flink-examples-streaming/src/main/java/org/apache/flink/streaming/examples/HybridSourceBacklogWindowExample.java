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

package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL_DURING_BACKLOG;

/** HybridSourceBacklogExample. */
public class HybridSourceBacklogWindowExample {
    public static void main(String[] args) throws Exception {
        final Configuration config = new Configuration();
        config.set(CHECKPOINTING_INTERVAL_DURING_BACKLOG, Duration.ZERO);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(2);

        final DataGeneratorSource<Tuple3<Integer, Long, Long>> historicalData =
                new DataGeneratorSource<>(
                        new GeneratorFunction<Long, Tuple3<Integer, Long, Long>>() {
                            @Override
                            public Tuple3<Integer, Long, Long> map(Long value) throws Exception {
                                Thread.sleep(500);
                                return new Tuple3<>(value.intValue() % 2, value / 2 * 1000, 1L);
                            }
                        },
                        10,
                        Types.TUPLE(Types.INT, Types.LONG));

        final DataGeneratorSource<Tuple3<Integer, Long, Long>> realTimeData =
                new DataGeneratorSource<>(
                        new GeneratorFunction<Long, Tuple3<Integer, Long, Long>>() {

                            @Override
                            public Tuple3<Integer, Long, Long> map(Long value) throws Exception {
                                Thread.sleep(500);
                                return new Tuple3<>(
                                        value.intValue() % 2, (value + 10) / 2 * 1000, 1L);
                            }
                        },
                        1000,
                        Types.TUPLE(Types.INT, Types.LONG));

        final HybridSource<Tuple3<Integer, Long, Long>> source =
                HybridSource.builder(historicalData).addSource(realTimeData).build();
        final SingleOutputStreamOperator<Tuple3<Integer, Long, Long>> summed =
                env.fromSource(
                                source,
                                WatermarkStrategy
                                        .<Tuple3<Integer, Long, Long>>forMonotonousTimestamps()
                                        .withTimestampAssigner((event, timestamp) -> event.f1),
                                "source")
                        .returns(Types.TUPLE(Types.INT, Types.LONG, Types.LONG))
                        .keyBy(record -> record.f0)
                        .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                        .sum(2);
        summed.print();
        env.execute();
    }
}
