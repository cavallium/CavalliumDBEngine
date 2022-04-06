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

import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.lucene.IArray;
import it.cavallium.dbengine.lucene.HugePqArray;
import it.cavallium.dbengine.lucene.LongCodec;
import java.io.IOException;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.comparators.NumericComparator;

/**
 * Comparator based on {@link Long#compare} for {@code numHits}. This comparator provides a skipping
 * functionality – an iterator that can skip over non-competitive documents.
 * Based on {@link org.apache.lucene.search.comparators.LongComparator}
 */
public class LongComparator extends NumericComparator<Long> implements SafeCloseable {
  private final IArray<Long> values;
  protected long topValue;
  protected long bottom;

  public LongComparator(LLTempHugePqEnv env,
      int numHits, String field, Long missingValue, boolean reverse, int sortPos) {
    super(field, missingValue != null ? missingValue : 0L, reverse, sortPos, Long.BYTES);
		values = new HugePqArray<>(env, new LongCodec(), numHits, 0L);
  }

  @Override
  public int compare(int slot1, int slot2) {
    return Long.compare(values.getOrDefault(slot1, 0L), values.getOrDefault(slot2, 0L));
  }

  @Override
  public void setTopValue(Long value) {
    super.setTopValue(value);
    topValue = value;
  }

  @Override
  public Long value(int slot) {
    return values.getOrDefault(slot, 0L);
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    return new LongLeafComparator(context);
  }

	@Override
	public void close() {
		if (values instanceof SafeCloseable closeable) {
			closeable.close();
		}
	}

	/** Leaf comparator for {@link LongComparator} that provides skipping functionality */
  public class LongLeafComparator extends NumericLeafComparator {

    public LongLeafComparator(LeafReaderContext context) throws IOException {
      super(context);
    }

    private long getValueForDoc(int doc) throws IOException {
      if (docValues.advanceExact(doc)) {
        return docValues.longValue();
      } else {
        return missingValue;
      }
    }

    @Override
    public void setBottom(int slot) throws IOException {
      bottom = values.getOrDefault(slot, 0L);
      super.setBottom(slot);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return Long.compare(bottom, getValueForDoc(doc));
    }

    @Override
    public int compareTop(int doc) throws IOException {
      return Long.compare(topValue, getValueForDoc(doc));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      values.set(slot, getValueForDoc(doc));
      super.copy(slot, doc);
    }

    @Override
    protected boolean isMissingValueCompetitive() {
      int result = Long.compare(missingValue, bottom);
      // in reverse (desc) sort missingValue is competitive when it's greater or equal to bottom,
      // in asc sort missingValue is competitive when it's smaller or equal to bottom
      return reverse ? (result >= 0) : (result <= 0);
    }

    @Override
    protected void encodeBottom(byte[] packedValue) {
      LongPoint.encodeDimension(bottom, packedValue, 0);
    }

    @Override
    protected void encodeTop(byte[] packedValue) {
      LongPoint.encodeDimension(topValue, packedValue, 0);
    }
  }
}
