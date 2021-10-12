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
package it.cavallium.dbengine.lucene.collector;

import it.cavallium.dbengine.lucene.EmptyPriorityQueue;
import it.cavallium.dbengine.lucene.FullDocs;
import it.cavallium.dbengine.lucene.LLDocElement;
import it.cavallium.dbengine.lucene.PqFullDocs;
import it.cavallium.dbengine.lucene.PriorityQueue;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * A base class for all collectors that return a {@link TopDocs} output. This collector allows easy
 * extension by providing a single constructor which accepts a {@link PriorityQueue} as well as
 * protected members for that priority queue and a counter of the number of total hits.<br>
 * Extending classes can override any of the methods to provide their own implementation, as well as
 * avoid the use of the priority queue entirely by passing null to {@link
 * #FullDocsCollector(PriorityQueue)}. In that case however, you might want to consider overriding
 * all methods, in order to avoid a NullPointerException.
 */
public abstract class FullDocsCollector<T extends LLDocElement> implements Collector, AutoCloseable {

	/**
	 * This is used in case topDocs() is called with illegal parameters, or there simply aren't
	 * (enough) results.
	 */
	private static final FullDocs<?> EMPTY_FULLDOCS =
			new PqFullDocs(new EmptyPriorityQueue<>(), new TotalHits(0, TotalHits.Relation.EQUAL_TO));

	/**
	 * The priority queue which holds the top documents. Note that different implementations of
	 * PriorityQueue give different meaning to 'top documents'. HitQueue for example aggregates the
	 * top scoring documents, while other PQ implementations may hold documents sorted by other
	 * criteria.
	 */
	protected final PriorityQueue<T> pq;

	/** The total number of documents that the collector encountered. */
	protected int totalHits;

	/** Whether {@link #totalHits} is exact or a lower bound. */
	protected TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;

	protected FullDocsCollector(PriorityQueue<T> pq) {
		this.pq = pq;
	}

	/** The total number of documents that matched this query. */
	public int getTotalHits() {
		return totalHits;
	}

	/** Returns the top docs that were collected by this collector. */
	public FullDocs<T> fullDocs() {
		return new PqFullDocs<>(this.pq, new TotalHits(totalHits, totalHitsRelation));
	}

	@Override
	public void close() throws Exception {
		pq.close();
	}
}