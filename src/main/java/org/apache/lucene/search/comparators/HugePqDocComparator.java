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

package org.apache.lucene.search.comparators;

import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.lucene.IArray;
import it.cavallium.dbengine.lucene.IntCodec;
import it.cavallium.dbengine.lucene.HugePqArray;
import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;

/**
 * Comparator that sorts by asc _doc
 * Based on {@link org.apache.lucene.search.comparators.DocComparator}
 * */
public class HugePqDocComparator extends org.apache.lucene.search.comparators.DocComparator implements SafeCloseable {
  private final IArray<Integer> docIDs;
  private final boolean enableSkipping; // if skipping functionality should be enabled
  private int bottom;
  private int topValue;
  private boolean topValueSet;
  private boolean bottomValueSet;
  private boolean hitsThresholdReached;

  /** Creates a new comparator based on document ids for {@code numHits} */
  public HugePqDocComparator(LLTempHugePqEnv env, int numHits, boolean reverse, int sortPost) {
		super(0, reverse, sortPost);
		this.docIDs = new HugePqArray<>(env, new IntCodec(), numHits, 0);
		// skipping functionality is enabled if we are sorting by _doc in asc order as a primary sort
		this.enableSkipping = (!reverse && sortPost == 0);
  }

  @Override
  public int compare(int slot1, int slot2) {
    // No overflow risk because docIDs are non-negative
    return docIDs.getOrDefault(slot1, 0) - docIDs.getOrDefault(slot2, 0);
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) {
		// TODO: can we "map" our docIDs to the current
		// reader? saves having to then subtract on every
		// compare call
    return new DocLeafComparator(context);
  }

  @Override
  public void setTopValue(Integer value) {
    topValue = value;
    topValueSet = true;
  }

  @Override
  public Integer value(int slot) {
    return docIDs.getOrDefault(slot, 0);
  }

	@Override
	public void close() {
		if (docIDs instanceof SafeCloseable closeable) {
			closeable.close();
		}
	}

	/**
   * DocLeafComparator with skipping functionality. When sort by _doc asc, after collecting top N
   * matches and enough hits, the comparator can skip all the following documents. When sort by _doc
   * asc and "top" document is set after which search should start, the comparator provides an
   * iterator that can quickly skip to the desired "top" document.
   */
  private class DocLeafComparator implements LeafFieldComparator {
    private final int docBase;
    private final int minDoc;
    private final int maxDoc;
    private DocIdSetIterator competitiveIterator; // iterator that starts from topValue

    public DocLeafComparator(LeafReaderContext context) {
      this.docBase = context.docBase;
      if (enableSkipping) {
        // Skip docs before topValue, but include docs starting with topValue.
        // Including topValue is necessary when doing sort on [_doc, other fields]
        // in a distributed search where there are docs from different indices
        // with the same docID.
        this.minDoc = topValue;
        this.maxDoc = context.reader().maxDoc();
        this.competitiveIterator = DocIdSetIterator.all(maxDoc);
      } else {
        this.minDoc = -1;
        this.maxDoc = -1;
        this.competitiveIterator = null;
      }
    }

    @Override
    public void setBottom(int slot) {
      bottom = docIDs.getOrDefault(slot, 0);
      bottomValueSet = true;
      updateIterator();
    }

    @Override
    public int compareBottom(int doc) {
      // No overflow risk because docIDs are non-negative
      return bottom - (docBase + doc);
    }

    @Override
    public int compareTop(int doc) {
      int docValue = docBase + doc;
      return Integer.compare(topValue, docValue);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      docIDs.set(slot, docBase + doc);
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      // update an iterator on a new segment
      updateIterator();
    }

    @Override
    public DocIdSetIterator competitiveIterator() {
      if (!enableSkipping) {
        return null;
      } else {
        return new DocIdSetIterator() {
          private int docID = competitiveIterator.docID();

          @Override
          public int nextDoc() throws IOException {
            return advance(docID + 1);
          }

          @Override
          public int docID() {
            return docID;
          }

          @Override
          public long cost() {
            return competitiveIterator.cost();
          }

          @Override
          public int advance(int target) throws IOException {
            return docID = competitiveIterator.advance(target);
          }
        };
      }
    }

    @Override
    public void setHitsThresholdReached() {
      hitsThresholdReached = true;
      updateIterator();
    }

    private void updateIterator() {
      if (!enableSkipping || !hitsThresholdReached) return;
      if (bottomValueSet) {
        // since we've collected top N matches, we can early terminate
        // Currently early termination on _doc is also implemented in TopFieldCollector, but this
        // will be removed
        // once all bulk scores uses collectors' iterators
        competitiveIterator = DocIdSetIterator.empty();
      } else if (topValueSet) {
        // skip to the desired top doc
        if (docBase + maxDoc <= minDoc) {
          competitiveIterator = DocIdSetIterator.empty(); // skip this segment
        } else {
          int segmentMinDoc = Math.max(competitiveIterator.docID(), minDoc - docBase);
          competitiveIterator = new MinDocIterator(segmentMinDoc, maxDoc);
        }
      }
    }
  }
}
