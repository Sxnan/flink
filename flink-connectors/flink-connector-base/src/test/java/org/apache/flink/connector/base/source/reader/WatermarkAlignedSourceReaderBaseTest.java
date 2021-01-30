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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.event.GlobalWatermarkEvent;
import org.apache.flink.connector.base.source.event.SourceReaderWatermarkEvent;
import org.apache.flink.connector.base.source.reader.fetcher.WatermarkAlignedSingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.WatermarkAlignedSplitFetcherManager;
import org.apache.flink.connector.base.source.reader.mocks.MockRecordEmitter;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.PausableSplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.SourceReaderTestBase;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.Preconditions;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link WatermarkAlignedSourceReaderBase}.
 */
public class WatermarkAlignedSourceReaderBaseTest extends SourceReaderTestBase<MockSourceSplit> {

	@Test
	public void testSplitsStatusReport() throws Exception {
		final MockSourceReaderContext sourceReaderContext = new MockSourceReaderContext();
		WatermarkAlignedSourceReaderBase<int[], Integer, MockSourceSplit, AtomicInteger>
			sourceReader = createReader(sourceReaderContext);

		ValidatingSourceOutput output = new WatermarkAlignedReaderOutput();
		MockSourceSplit split = new MockSourceSplit(0);

		sourceReader.start();
		sourceReader.addSplits(Collections.singletonList(split));
		addRecordToSplit(split, 1000);

		while (output.count() < 1) {
			sourceReader.pollNext(output);
		}

		Thread.sleep(1000);
		final SourceEvent lastSentEvent = sourceReaderContext.getLastSentEvent();
		assertTrue(lastSentEvent instanceof SourceReaderWatermarkEvent);
		assertEquals(Long.MIN_VALUE,
			((SourceReaderWatermarkEvent) lastSentEvent).getWatermark().longValue());
	}

	@Test
	public void testHandleGlobalWatermarkEvent() throws Exception {
		final MockSourceReaderContext sourceReaderContext = new MockSourceReaderContext();
		FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
				new FutureCompletingBlockingQueue<>();
		WatermarkAlignedSingleThreadFetcherManager<int[], MockSourceSplit> fetcherManager =
				new WatermarkAlignedSingleThreadFetcherManager<>(
						elementsQueue,
						() ->
								new MockPausableSplitReader(1, true));
		fetcherManager = Mockito.spy(fetcherManager);
		WatermarkAlignedSourceReaderBase<int[], Integer, MockSourceSplit, AtomicInteger>
			sourceReader = createReader(elementsQueue, fetcherManager, sourceReaderContext);

		ValidatingSourceOutput output = new WatermarkAlignedReaderOutput();
		MockSourceSplit split = new MockSourceSplit(0);

		sourceReader.start();
		sourceReader.addSplits(Collections.singletonList(split));
		addRecordToSplit(split, 1000);

		while (output.count() < 1) {
			sourceReader.pollNext(output);
		}
		Map<String, Long> splitStatuses = sourceReader.getAllSplitWatermarks();
		assertEquals(1, splitStatuses.size());
		output.emitWatermark(new Watermark(1000L));
		sourceReader.handleSourceEvents(new GlobalWatermarkEvent(0L));

		final ArgumentCaptor<Map<String, PausableSplitReader.SplitState>>
				mapArgumentCaptor = ArgumentCaptor.forClass(Map.class);
		Mockito.verify(fetcherManager).updateSplitsState(mapArgumentCaptor.capture());
		final Map<String, PausableSplitReader.SplitState> value = mapArgumentCaptor.getValue();
		assertEquals(PausableSplitReader.SplitState.PAUSE, value.get("0"));
	}

	private void addRecordToSplit(MockSourceSplit split, int numOfRecord) {
		addRecordToSplit(split, 0, numOfRecord);
	}

	private void addRecordToSplit(MockSourceSplit split, int startVal, int endVal) {
		for (int i = startVal; i < endVal; ++i) {
			split.addRecord(i);
		}
	}

	@Override
	public WatermarkAlignedSourceReaderBase<int[], Integer, MockSourceSplit, AtomicInteger> createReader() {
		FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
				new FutureCompletingBlockingQueue<>();
		return createReader(
				elementsQueue,
				new WatermarkAlignedSingleThreadFetcherManager<>(elementsQueue, () ->
						new MockPausableSplitReader(1, true)),
				null);
	}

	private WatermarkAlignedSourceReaderBase<int[], Integer, MockSourceSplit, AtomicInteger> createReader(
			MockSourceReaderContext sourceReaderContext) {
		FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
			new FutureCompletingBlockingQueue<>();
		return createReader(
				elementsQueue,
				new WatermarkAlignedSingleThreadFetcherManager<>(elementsQueue, () ->
						new MockPausableSplitReader(1, true)),
				sourceReaderContext);
	}

	private WatermarkAlignedSourceReaderBase<int[], Integer, MockSourceSplit, AtomicInteger> createReader(
			FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue,
			WatermarkAlignedSplitFetcherManager<int[], MockSourceSplit> splitFetcherManager,
			SourceReaderContext sourceReaderContext) {
		return new MockWatermarkAlignedSourceReaderBase(
				elementsQueue,
				splitFetcherManager,
				new MockRecordEmitter(),
				getConfig(),
				sourceReaderContext
		);
	}

	@Override
	protected List<MockSourceSplit> getSplits(int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
		List<MockSourceSplit> mockSplits = new ArrayList<>();
		for (int i = 0; i < numSplits; i++) {
			mockSplits.add(getSplit(i, numRecordsPerSplit, boundedness));
		}
		return mockSplits;
	}

	@Override
	protected MockSourceSplit getSplit(int splitId, int numRecords, Boundedness boundedness) {
		MockSourceSplit mockSplit;
		if (boundedness == Boundedness.BOUNDED) {
			mockSplit = new MockSourceSplit(splitId, 0, numRecords);
		} else {
			mockSplit = new MockSourceSplit(splitId);
		}
		for (int j = 0; j < numRecords; j++) {
			mockSplit.addRecord(splitId * 10 + j);
		}
		return mockSplit;
	}

	@Override
	protected long getNextRecordIndex(MockSourceSplit split) {
		return split.index();
	}

	private Configuration getConfig() {
		Configuration config = new Configuration();
		config.setInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1);
		config.setLong(SourceReaderOptions.SOURCE_READER_CLOSE_TIMEOUT, 30000L);
		return config;
	}

	static class MockWatermarkAlignedSourceReaderBase
		extends WatermarkAlignedSourceReaderBase<int[], Integer, MockSourceSplit, AtomicInteger> {

		public MockWatermarkAlignedSourceReaderBase(
			FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue,
			WatermarkAlignedSplitFetcherManager<int[], MockSourceSplit> splitFetcherManager,
			RecordEmitter<int[], Integer, AtomicInteger> recordEmitter,
			Configuration config,
			SourceReaderContext context) {
			super(elementsQueue, splitFetcherManager, recordEmitter, config, context, 1, 1);
		}

		@Override
		protected void onSplitFinished(Map<String, AtomicInteger> finishedSplitIds) {

		}

		@Override
		protected AtomicInteger initializedState(MockSourceSplit split) {
			return new AtomicInteger(split.index());
		}

		@Override
		protected MockSourceSplit toSplitType(String splitId, AtomicInteger splitState) {
			return new MockSourceSplit(Integer.parseInt(splitId), splitState.get());
		}
	}

	private static class MockPausableSplitReader extends MockSplitReader
		implements PausableSplitReader<int[], MockSourceSplit> {
		Map<String, MockSourceSplit> pausedSplit = new HashMap<>();

		public MockPausableSplitReader(int numRecordsPerSplitPerFetch, boolean blockingFetch) {
			super(numRecordsPerSplitPerFetch, blockingFetch);
		}

		@Override
		public void pause(Set<String> splitsToPause) {
			for (String splitId : splitsToPause) {
				final MockSourceSplit pausingSplit = splits.remove(splitId);
				Preconditions.checkNotNull(pausingSplit);
				pausedSplit.put(splitId, pausingSplit);
			}
		}

		@Override
		public void resume(Set<String> splitsToResume) {
			for (String splitId : splitsToResume) {
				final MockSourceSplit resumingSplit = pausedSplit.get(splitId);
				Preconditions.checkNotNull(resumingSplit);
				splits.put(splitId, resumingSplit);
			}
		}

		@Override
		public Map<String, SplitState> getSplitState() {
			Map<String, SplitState> res = new HashMap<>();
			splits.keySet().forEach(splitId -> res.put(splitId, SplitState.RUN));
			pausedSplit.keySet().forEach(splitID -> res.put(splitID, SplitState.PAUSE));
			return res;
		}
	}

	private static class MockSourceReaderContext implements SourceReaderContext {
		private SourceEvent lastSentEvent;

		@Override
		public MetricGroup metricGroup() {
			return new UnregisteredMetricsGroup();
		}

		@Override
		public Configuration getConfiguration() {
			return new Configuration();
		}

		@Override
		public String getLocalHostName() {
			return "localhost";
		}

		@Override
		public String getTaskName() {
			return "Mock";
		}

		@Override
		public int getIndexOfSubtask() {
			return 0;
		}

		@Override
		public void sendSplitRequest() {}

		@Override
		public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {
			lastSentEvent = sourceEvent;
		}

		public SourceEvent getLastSentEvent() {
			return lastSentEvent;
		}
	}

	private static class WatermarkAlignedReaderOutput extends ValidatingSourceOutput {

		private Long lastEmittedWatermark = Long.MIN_VALUE;

		@Override
		public void emitWatermark(Watermark watermark) {
			lastEmittedWatermark = watermark.getTimestamp();
		}

		@Override
		public Long getCurrentWatermark() {
			return lastEmittedWatermark;
		}
	}
}
