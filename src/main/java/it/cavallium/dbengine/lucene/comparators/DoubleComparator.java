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
import it.cavallium.dbengine.lucene.DoubleCodec;
import it.cavallium.dbengine.lucene.IArray;
import it.cavallium.dbengine.lucene.HugePqArray;
import java.io.IOException;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.comparators.NumericComparator;

/**
 * Comparator based on {@link Double#compare} for {@code numHits}. This comparator provides a
 * skipping functionality - an iterator that can skip over non-competitive documents.
 * Based on {@link org.apache.lucene.search.comparators.DoubleComparator}
 */
public class DoubleComparator extends NumericComparator<Double> implements DiscardingCloseable {
  private final IArray<Double> values;
  protected double topValue;
  protected double bottom;

  public DoubleComparator(LLTempHugePqEnv env,
      int numHits, String field, Double missingValue, boolean reverse, boolean enableSkipping) {
    super(field, missingValue != null ? missingValue : 0.0, reverse, enableSkipping, Double.BYTES);
		values = new HugePqArray<>(env, new DoubleCodec(), numHits, 0d);
  }

  @Override
  public int compare(int slot1, int slot2) {
		var value1 = values.get(slot1);
		var value2 = values.get(slot2);
		assert value1 != null : "Missing value for slot1: " + slot1;
		assert value2 != null : "Missing value for slot2: " + slot2;
    return Double.compare(value1, value2);
  }

  @Override
  public void setTopValue(Double value) {
    super.setTopValue(value);
    topValue = value;
  }

  @Override
  public Double value(int slot) {
    return values.getOrDefault(slot, 0d);
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    return new DoubleLeafComparator(context);
  }

	@Override
	public void close() {
		if (values instanceof SafeCloseable closeable) {
			closeable.close();
		}
	}

	/** Leaf comparator for {@link DoubleComparator} that provides skipping functionality */
  public class DoubleLeafComparator extends NumericLeafComparator {

    public DoubleLeafComparator(LeafReaderContext context) throws IOException {
      super(context);
    }

    private double getValueForDoc(int doc) throws IOException {
      if (docValues.advanceExact(doc)) {
        return Double.longBitsToDouble(docValues.longValue());
      } else {
        return missingValue;
      }
    }

    @Override
    public void setBottom(int slot) throws IOException {
			bottom = values.getOrDefault(slot, 0d);
      super.setBottom(slot);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return Double.compare(bottom, getValueForDoc(doc));
    }

    @Override
    public int compareTop(int doc) throws IOException {
      return Double.compare(topValue, getValueForDoc(doc));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      values.set(slot, getValueForDoc(doc));
      super.copy(slot, doc);
    }

    @Override
    protected boolean isMissingValueCompetitive() {
      int result = Double.compare(missingValue, bottom);
      return reverse ? (result >= 0) : (result <= 0);
    }

    @Override
    protected void encodeBottom(byte[] packedValue) {
      DoublePoint.encodeDimension(bottom, packedValue, 0);
    }

    @Override
    protected void encodeTop(byte[] packedValue) {
      DoublePoint.encodeDimension(topValue, packedValue, 0);
    }
  }
}
