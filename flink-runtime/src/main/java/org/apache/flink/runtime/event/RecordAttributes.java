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

package org.apache.flink.runtime.event;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A RecordAttributes element provides stream task with information that can be used to optimize the
 * stream task's performance.
 */
@PublicEvolving
public class RecordAttributes extends RuntimeEvent {

    private boolean backlog;

    public RecordAttributes() {}

    public RecordAttributes(boolean backlog) {
        this.backlog = backlog;
    }

    /**
     * If it returns true, then the records received after this element are stale and an operator
     * can optionally buffer records until isBacklog=false. This allows an operator to optimize
     * throughput at the cost of processing latency.
     */
    public boolean isBacklog() {
        return backlog;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeBoolean(backlog);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        this.backlog = in.readBoolean();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("RecordAttributes{");
        sb.append("backlog=").append(backlog);
        sb.append('}');
        return sb.toString();
    }
}
