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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

/** Indicates some task fail to consume cached intermediate dataset. */
public class CacheCorruptedException extends JobException {
    private final IntermediateDataSetID corruptedIntermediateDataSetID;

    public CacheCorruptedException(
            String message, Throwable cause, IntermediateDataSetID corruptedIntermediateDataSetID) {
        super(
                String.format(
                        "%s. Corrupted intermediate dataset ID: %s",
                        message, corruptedIntermediateDataSetID),
                cause);
        this.corruptedIntermediateDataSetID = corruptedIntermediateDataSetID;
    }

    public IntermediateDataSetID getCorruptedIntermediateDataSetID() {
        return corruptedIntermediateDataSetID;
    }
}
