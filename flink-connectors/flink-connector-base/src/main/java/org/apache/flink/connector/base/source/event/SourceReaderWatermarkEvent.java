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

package org.apache.flink.connector.base.source.event;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.connector.base.source.splitenumerator.WatermarkAlignedSplitEnumerator;

/**
 * The SourceReaderWatermarkEvent is sent to {@link WatermarkAlignedSplitEnumerator} to update the
 * current watermark of the SourceReader. The current watermark is typically the minimal among the
 * watermarks from all the splits in the source reader.
 */
public class SourceReaderWatermarkEvent implements SourceEvent {
    private final Long watermark;

    public SourceReaderWatermarkEvent(Long watermark) {
        this.watermark = watermark;
    }

    public Long getWatermark() {
        return watermark;
    }
}
