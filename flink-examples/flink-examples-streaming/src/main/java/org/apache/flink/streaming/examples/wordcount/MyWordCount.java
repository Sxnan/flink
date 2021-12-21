package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.SplittableIterator;

import java.util.ArrayList;
import java.util.Iterator;

public class MyWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);


        DataStream<String> source = env.fromParallelCollection(new SplittableIterator<String>() {

            private ArrayList<String> strings = Lists.newArrayList("a", "b", "c", "a");;

            @Override
            public Iterator<String>[] split(int numPartitions) {
                final Iterator<String>[] res = new Iterator[numPartitions];
                for (int i = 0; i < numPartitions; ++i) {
                    res[i] = strings.iterator();
                }
                return res;
            }

            @Override
            public int getMaximumNumberOfSplits() {
                return 10;
            }

            @Override
            public boolean hasNext() {
                return strings.iterator().hasNext();
            }

            @Override
            public String next() {
                return strings.iterator().next();
            }
        }, String.class);
        source = source.cache();

        final DataStream<Tuple2<String, Integer>> map = source.map(
                w -> new Tuple2<>(w, 1),
                TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class)).name("Map to tuple").cache();
        map.print();
        map.keyBy(value -> value.f0)
                .reduce((v1, v2) -> new Tuple2<>(v1.f0, v1.f1 + v2.f1))
                .print();



        env.execute();

        map.print();
        map
            .keyBy(value -> value.f1)
            .reduce((v1, v2) -> new Tuple2<>(v1.f0 + v2.f0, v1.f1))
            .print();

        env.execute();
    }
}
