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

package org.apache.flink.connector.base.source.reader.splitreader;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Map;
import java.util.Set;

/** SplitReader that is able to pause and resume reading from splits. */
public interface PausableSplitReader<E, SplitT extends SourceSplit> extends SplitReader<E, SplitT> {
    /**
     * Stop the splits given by splitsToPause.
     *
     * @param splitsToPause the split ids of the splits to pause
     */
    void pause(Set<String> splitsToPause);

    /**
     * Resume the splits given by splitsToResume.
     *
     * @param splitsToResume the split ids of the splits to resume
     */
    void resume(Set<String> splitsToResume);

    /**
     * Get the mapping from split id to the state of the split.
     *
     * @return mapping from split id to the state of the split
     */
    Map<String, SplitState> getSplitState();

    /** The state of the split, which could be RUN or STOP. */
    enum SplitState {
        RUN,
        PAUSE
    }
}
