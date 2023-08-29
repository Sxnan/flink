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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.event.RecordAttributes;
import org.apache.flink.runtime.event.RecordAttributesBuilder;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;

import java.util.Collections;

/** RecordAttributesValve. */
public class RecordAttributesValve {
    private final int numInputChannels;

    private final RecordAttributes[] allChannelRecordAttributes;

    private int backlogChannelsCnt = 0;

    private RecordAttributes lastOutputAttributes = null;

    public RecordAttributesValve(int numInputChannels) {
        this.numInputChannels = numInputChannels;
        this.allChannelRecordAttributes = new RecordAttributes[numInputChannels];
    }

    public boolean inputRecordAttributes(
            RecordAttributes recordAttributes, int channelIdx, DataOutput<?> output)
            throws Exception {
        RecordAttributes lastChannelRecordAttributes = allChannelRecordAttributes[channelIdx];
        allChannelRecordAttributes[channelIdx] = recordAttributes;

        if (lastChannelRecordAttributes == null) {
            lastChannelRecordAttributes =
                    new RecordAttributesBuilder(Collections.emptyList()).build();
        }

        if (lastChannelRecordAttributes.isBacklog() == recordAttributes.isBacklog()) {
            return false;
        }

        if (recordAttributes.isBacklog()) {
            backlogChannelsCnt += 1;
        } else {
            backlogChannelsCnt -= 1;
        }

        final RecordAttributesBuilder builder =
                new RecordAttributesBuilder(Collections.emptyList());
        if (backlogChannelsCnt < numInputChannels) {
            builder.setBacklog(false);
        } else {
            builder.setBacklog(true);
        }

        final RecordAttributes outputAttribute = builder.build();

        if (lastOutputAttributes == null
                || lastOutputAttributes.isBacklog() != outputAttribute.isBacklog()) {
            lastOutputAttributes = outputAttribute;
            output.emitRecordAttributes(outputAttribute);
            return true;
        }

        return false;
    }
}
