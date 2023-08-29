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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** The builder class for {@link OperatorAttributes}. */
@Internal
public class OperatorAttributesBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorAttributesBuilder.class);

    @Nullable private Boolean internalSorterSupported = null;

    public OperatorAttributesBuilder() {}

    public OperatorAttributesBuilder setInternalSorterSupported(boolean internalSorterSupported) {
        this.internalSorterSupported = internalSorterSupported;
        return this;
    }

    /**
     * If any operator attribute is null, we will log it at DEBUG level and use the following
     * default values.
     *
     * <ul>
     *   <li>internalSorterSupported defaults to false
     * </ul>
     */
    public OperatorAttributes build() {
        return new OperatorAttributes(
                getAttributeOrDefaultValue(
                        internalSorterSupported, "internalSorterSupported", false));
    }

    private <T> T getAttributeOrDefaultValue(
            @Nullable T attribute, String attributeName, T defaultValue) {
        if (attribute == null) {
            LOG.debug("{} is not set, set it to default value {}.", attributeName, defaultValue);
            return defaultValue;
        }
        return attribute;
    }
}
