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

package org.apache.flink.connector.base.source.splitenumerator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.base.source.event.GlobalWatermarkEvent;
import org.apache.flink.connector.base.source.event.SourceReaderWatermarkEvent;

import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for {@link WatermarkAlignedSplitEnumerator}.
 */
public class WatermarkAlignedSplitEnumeratorTest {

	@Test
	public void testSendGlobalWatermarkEvent() throws Exception {
		final MockSplitEnumeratorContext<MockSourceSplit> context =
			new MockSplitEnumeratorContext<>(1);
		final MockWatermarkAlignedSplitEnumerator splitEnumerator =
			new MockWatermarkAlignedSplitEnumerator(context, 1);
		splitEnumerator.start();
		splitEnumerator.addReader(0);
		splitEnumerator.handleSourceEvent(0, new SourceReaderWatermarkEvent(100L));
		final List<Callable<Future<?>>> periodicCallables = context.getPeriodicCallables();
		assertEquals(1, periodicCallables.size());
		try {
			periodicCallables.get(0).call().get();
		} catch (Exception e) {
			fail("Fail to handle the source event for watermark alignment.");
		}
		final List<SourceEvent> sourceEvents = context.getSentSourceEvent().get(0);
		assertEquals(1, sourceEvents.size());
		final SourceEvent sourceEvent = sourceEvents.get(0);
		assertTrue(sourceEvent instanceof GlobalWatermarkEvent);
		assertEquals(100L,
			((GlobalWatermarkEvent) sourceEvent).getGlobalWatermark().longValue());
	}

	static class MockWatermarkAlignedSplitEnumerator
		extends WatermarkAlignedSplitEnumerator<MockSourceSplit, Integer> {

		public MockWatermarkAlignedSplitEnumerator(
			SplitEnumeratorContext<MockSourceSplit> context,
			long period) {
			super(context, period);
		}

		@Override
		public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
			// do nothing
		}

		@Override
		public void addSplitsBack(List<MockSourceSplit> splits, int subtaskId) {
			// do nothing
		}

		@Override
		public Integer snapshotState() throws Exception {
			return null;
		}

		@Override
		public void close() throws IOException {

		}
	}
}
