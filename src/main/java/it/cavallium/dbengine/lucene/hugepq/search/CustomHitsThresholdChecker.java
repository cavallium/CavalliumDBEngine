/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.cavallium.dbengine.lucene.hugepq.search;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.search.ScoreMode;

/** Used for defining custom algorithms to allow searches to early terminate */
public abstract class CustomHitsThresholdChecker {
	/** Implementation of CustomHitsThresholdChecker which allows global hit counting */
	private static class GlobalHitsThresholdChecker extends CustomHitsThresholdChecker {
		private final long totalHitsThreshold;
		private final AtomicLong globalHitCount;

		public GlobalHitsThresholdChecker(long totalHitsThreshold) {

			if (totalHitsThreshold < 0) {
				throw new IllegalArgumentException(
						"totalHitsThreshold must be >= 0, got " + totalHitsThreshold);
			}

			this.totalHitsThreshold = totalHitsThreshold;
			this.globalHitCount = new AtomicLong();
		}

		@Override
		public void incrementHitCount() {
			globalHitCount.incrementAndGet();
		}

		@Override
		public boolean isThresholdReached(boolean supports64Bit) {
			if (supports64Bit) {
				return globalHitCount.getAcquire() > totalHitsThreshold;
			} else {
				return Math.min(globalHitCount.getAcquire(), Integer.MAX_VALUE) > Math.min(totalHitsThreshold, Integer.MAX_VALUE);
			}
		}

		@Override
		public ScoreMode scoreMode() {
			if (totalHitsThreshold == Long.MAX_VALUE) {
				return ScoreMode.COMPLETE;
			}
			return ScoreMode.TOP_SCORES;
		}

		@Override
		public long getHitsThreshold(boolean supports64Bit) {
			if (supports64Bit) {
				return totalHitsThreshold;
			} else {
				return Math.min(totalHitsThreshold, Integer.MAX_VALUE);
			}
		}
	}

	/** Default implementation of CustomHitsThresholdChecker to be used for single threaded execution */
	private static class LocalHitsThresholdChecker extends CustomHitsThresholdChecker {
		private final long totalHitsThreshold;
		private long hitCount;

		public LocalHitsThresholdChecker(long totalHitsThreshold) {

			if (totalHitsThreshold < 0) {
				throw new IllegalArgumentException(
						"totalHitsThreshold must be >= 0, got " + totalHitsThreshold);
			}

			this.totalHitsThreshold = totalHitsThreshold;
		}

		@Override
		public void incrementHitCount() {
			++hitCount;
		}

		@Override
		public boolean isThresholdReached(boolean supports64Bit) {
			if (supports64Bit) {
				return hitCount > totalHitsThreshold;
			} else {
				return Math.min(hitCount, Integer.MAX_VALUE) > Math.min(totalHitsThreshold, Integer.MAX_VALUE);
			}
		}

		@Override
		public ScoreMode scoreMode() {
			if (totalHitsThreshold == Long.MAX_VALUE) {
				return ScoreMode.COMPLETE;
			}
			return ScoreMode.TOP_SCORES;
		}

		@Override
		public long getHitsThreshold(boolean supports64Bit) {
			if (supports64Bit) {
				return totalHitsThreshold;
			} else {
				return Math.min(totalHitsThreshold, Integer.MAX_VALUE);
			}
		}
	}

	/*
	 * Returns a threshold checker that is useful for single threaded searches
	 */
	public static CustomHitsThresholdChecker create(final long totalHitsThreshold) {
		return new LocalHitsThresholdChecker(totalHitsThreshold);
	}

	/*
	 * Returns a threshold checker that is based on a shared counter
	 */
	public static CustomHitsThresholdChecker createShared(final long totalHitsThreshold) {
		return new GlobalHitsThresholdChecker(totalHitsThreshold);
	}

	public abstract void incrementHitCount();

	public abstract ScoreMode scoreMode();

	public abstract long getHitsThreshold(boolean supports64Bit);

	public abstract boolean isThresholdReached(boolean supports64Bit);
}