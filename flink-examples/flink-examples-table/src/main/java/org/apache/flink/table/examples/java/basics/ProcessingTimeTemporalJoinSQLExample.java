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

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Example for demonstrating the use of temporal join between a table backed by a {@link DataStream}
 * and a table backed by a change log stream.
 *
 * <p>In particular, the example shows how to
 *
 * <ul>
 *   <li>create a change log stream from elements
 *   <li>rename the table columns
 *   <li>register a table as a view under a name,
 *   <li>run a stream temporal join query on registered tables,
 *   <li>and convert the table back to a data stream.
 * </ul>
 *
 * <p>The example executes a single Flink job. The results are written to stdout.
 */
public class ProcessingTimeTemporalJoinSQLExample {

    public static void main(String[] args) throws Exception {

        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(5000);

        // set up the Java Table API
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE `user_info` (\n"
                        + "    `id` INTEGER PRIMARY KEY NOT ENFORCED, \n"
                        + "    `gender` STRING, \n"
                        + "    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL"
                        + ") WITH (\n"
                        + "    'connector' = 'mysql-cdc',\n"
                        + "    'database-name' = 'flink_cdc',\n"
                        + "    'hostname' = 'localhost',\n"
                        + "    'username' = 'sxnan',\n"
                        + "    'password' = 'sxnan',\n"
                        + "    'scan.incremental.snapshot.chunk.size' = '2',\n"
                        + "    'table-name' = 'user_info'\n"
                        + ");");

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE `event` (\n"
                        + "    `id` INTEGER PRIMARY KEY NOT ENFORCED, \n"
                        + "    `proctime` AS PROCTIME()\n"
                        + ") WITH (\n"
                        + "    'connector' = 'datagen',\n"
                        + "    'rows-per-second' = '1',\n"
                        + "    'fields.id.min' = '0',\n"
                        + "    'fields.id.max' = '10'\n"
                        + ");");

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE `print_sink` (\n"
                        + "    `id` INTEGER,\n"
                        + "    `gender` STRING\n"
                        + ") WITH (\n"
                        + "    'connector' = 'print'\n"
                        + ");");

        tEnv.executeSql(
                        "INSERT INTO `print_sink`\n"
                                + "SELECT `event`.`id` AS `id`, `gender` FROM `event` \n"
                                + "LEFT JOIN `user_info` FOR SYSTEM_TIME AS OF `event`.`proctime`\n"
                                + "ON `event`.`id` = `user_info`.`id`;")
                .await();
    }
}
