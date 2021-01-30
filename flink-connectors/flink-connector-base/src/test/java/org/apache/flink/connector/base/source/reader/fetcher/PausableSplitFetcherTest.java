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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.PausableSplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.PausableSplitReader.SplitState;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Unit test for {@link PausableSplitFetcher}.
 */
public class PausableSplitFetcherTest {
	@Test
	public void testPause() throws InterruptedException {
		final int splitId = 0;
		FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementQueue =
			new FutureCompletingBlockingQueue<>();
		PausableSplitFetcher<int[], MockSourceSplit> fetcher =
			new PausableSplitFetcher<>(
				0,
				elementQueue,
				new PausableMockSplitReader(1, true),
				(err) -> {},
				() -> {});

		// Prepare the splits.
		MockSourceSplit split = new MockSourceSplit(splitId);
		List<MockSourceSplit> splits = Collections.singletonList(split);

		// Add splits to the fetcher.
		fetcher.addSplits(splits);

		// A thread drives the fetcher.
		Thread fetcherThread = new Thread(fetcher, "FetcherThread");

		try {
			fetcherThread.start();
			split.addRecord(1);
			RecordsWithSplitIds<int[]> records = elementQueue.take();
			assertEquals(String.valueOf(splitId), records.nextSplit());
			assertNotNull(records.nextRecordFromSplit());

			// sleep for a short time so that the MockSplitReader will be blocked on fetching the data
			Thread.sleep(100);

			// pause the split
			fetcher.updateSplitStates(Collections.singletonMap(
				String.valueOf(splitId),
				SplitState.PAUSE));
			// sleep for a short time to ensure that the fetcher thread is interrupted
			Thread.sleep(100);
			split.addRecord(2);
			records = elementQueue.take();
			assertNull(records.nextSplit());

			fetcher.updateSplitStates(Collections.singletonMap(
				String.valueOf(splitId),
				SplitState.RUN));
			String nextSplit;
			do {
				records = elementQueue.take();
				nextSplit = records.nextSplit();
			} while (nextSplit == null);
			assertNotNull(records.nextRecordFromSplit());
		} finally {
			fetcher.shutdown();
			fetcherThread.join();
		}
	}

	/**
	 * A pausable split reader that read {@link MockSourceSplit}.
	 */
	public static class PausableMockSplitReader extends MockSplitReader
		implements PausableSplitReader<int[], MockSourceSplit> {

		protected final Map<String, MockSourceSplit> pauseSplits = new LinkedHashMap<>();

		public PausableMockSplitReader(int numRecordsPerSplitPerFetch, boolean blockingFetch) {
			super(numRecordsPerSplitPerFetch, blockingFetch);
		}

		@Override
		public void pause(Set<String> splitsToPause) {
			splitsToPause.forEach(splitId -> {
				final MockSourceSplit pauseSplit = splits.remove(splitId);
				Preconditions.checkNotNull(pauseSplit);
				pauseSplits.put(splitId, pauseSplit);
			});
		}

		@Override
		public void resume(Set<String> splitsToResume) {
			splitsToResume.forEach(splitId -> {
				final MockSourceSplit resumeSplit = pauseSplits.remove(splitId);
				Preconditions.checkNotNull(resumeSplit);
				splits.put(splitId, resumeSplit);
			});
		}

		@Override
		public Map<String, SplitState> getSplitState() {
			Map<String, SplitState> res = new HashMap<>();
			splits.keySet().forEach(splitId -> res.put(splitId, SplitState.RUN));
			pauseSplits.keySet().forEach(splitId -> res.put(splitId, SplitState.PAUSE));
			return res;
		}
	}
}
