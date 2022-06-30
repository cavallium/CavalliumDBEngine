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
package it.cavallium.dbengine.lucene.comparators;

import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.lucene.FloatCodec;
import it.cavallium.dbengine.lucene.IArray;
import it.cavallium.dbengine.lucene.HugePqArray;
import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreCachingWrappingScorer;

/**
 * Sorts by descending relevance. NOTE: if you are sorting only by descending relevance and then secondarily by
 * ascending docID, performance is faster using {@link org.apache.lucene.search.TopScoreDocCollector} directly (which {@link
 * org.apache.lucene.search.IndexSearcher#search(Query, int)} uses when no {@link org.apache.lucene.search.Sort} is specified).
 * Based on {@link org.apache.lucene.search.FieldComparator.RelevanceComparator}
 */
public final class RelevanceComparator extends FieldComparator<Float> implements LeafFieldComparator,
		DiscardingCloseable {

	private final IArray<Float> scores;
	private float bottom;
	private Scorable scorer;
	private float topValue;

	/**
	 * Creates a new comparator based on relevance for {@code numHits}.
	 */
	public RelevanceComparator(LLTempHugePqEnv env, int numHits) {
		scores = new HugePqArray<>(env, new FloatCodec(), numHits, 0f);
	}

	@Override
	public int compare(int slot1, int slot2) {
		var value1 = scores.get(slot1);
		var value2 = scores.get(slot2);
		assert value1 != null : "Missing score for slot1: " + slot1;
		assert value2 != null : "Missing score for slot2: " + slot2;
		return Float.compare(value1, value2);
	}

	@Override
	public int compareBottom(int doc) throws IOException {
		float score = scorer.score();
		assert !Float.isNaN(score);
		return Float.compare(score, bottom);
	}

	@Override
	public void copy(int slot, int doc) throws IOException {
		var score = scorer.score();
		scores.set(slot, score);
		assert !Float.isNaN(score);
	}

	@Override
	public LeafFieldComparator getLeafComparator(LeafReaderContext context) {
		return this;
	}

	@Override
	public void setBottom(final int bottom) {
		this.bottom = scores.getOrDefault(bottom, 0f);
	}

	@Override
	public void setTopValue(Float value) {
		topValue = value;
	}

	@Override
	public void setScorer(Scorable scorer) {
		// wrap with a ScoreCachingWrappingScorer so that successive calls to
		// score() will not incur score computation over and
		// over again.
		this.scorer = ScoreCachingWrappingScorer.wrap(scorer);
	}

	@Override
	public Float value(int slot) {
		return scores.getOrDefault(slot, 0f);
	}

	// Override because we sort reverse of natural Float order:
	@Override
	public int compareValues(Float first, Float second) {
		// Reversed intentionally because relevance by default
		// sorts descending:
		return second.compareTo(first);
	}

	@Override
	public int compareTop(int doc) throws IOException {
		float docValue = scorer.score();
		assert !Float.isNaN(docValue);
		return Float.compare(docValue, topValue);
	}

	@Override
	public void close() {
		if (this.scores instanceof SafeCloseable closeable) {
			closeable.close();
		}
	}
}
