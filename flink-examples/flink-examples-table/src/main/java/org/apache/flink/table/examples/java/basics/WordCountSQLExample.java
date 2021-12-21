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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/** The famous word count example that shows a minimal Flink SQL job in batch execution mode. */
public final class WordCountSQLExample {

    public static void main(String[] args) throws Exception {

        // set up the Table API
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("CREATE TABLE source (id INT, proc_time AS PROCTIME())"
                + "WITH"
                + "('connector'='datagen', 'fields.id.kind'='sequence', 'fields.id.start'='0', 'fields.id.end'='100', 'rows-per-second'='1')");

        // execute a Flink SQL job and print the result locally
        tableEnv.executeSql("CREATE TEMPORARY VIEW id_view AS SELECT id, UNIX_TIMESTAMP() as ts FROM source");

        tableEnv.executeSql("SELECT id, ts FROM id_view WHERE ts % 2 <> 0")
                .print();
    }
}
