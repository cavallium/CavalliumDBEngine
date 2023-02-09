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

import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.lucene.FullDocs;
import it.cavallium.dbengine.lucene.LLDoc;
import it.cavallium.dbengine.lucene.LazyFullDocs;
import it.cavallium.dbengine.lucene.PriorityQueue;
import it.cavallium.dbengine.lucene.ResourceIterable;
import it.cavallium.dbengine.lucene.Reversable;
import it.cavallium.dbengine.lucene.ReversableResourceIterable;
import it.cavallium.dbengine.utils.SimpleResource;
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
public abstract class FullDocsCollector<PQ extends PriorityQueue<INTERNAL> & Reversable<ReversableResourceIterable<INTERNAL>>, INTERNAL extends LLDoc,
		EXTERNAL extends LLDoc> extends SimpleResource implements Collector, DiscardingCloseable {

	/**
	 * The priority queue which holds the top documents. Note that different implementations of
	 * PriorityQueue give different meaning to 'top documents'. HitQueue for example aggregates the
	 * top scoring documents, while other PQ implementations may hold documents sorted by other
	 * criteria.
	 */
	protected final PQ pq;

	/** The total number of documents that the collector encountered. */
	protected int totalHits;

	/** Whether {@link #totalHits} is exact or a lower bound. */
	protected TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;

	protected FullDocsCollector(PQ pq) {
		this.pq = pq;
	}

	/** The total number of documents that matched this query. */
	public int getTotalHits() {
		return totalHits;
	}

	/** Returns the top docs that were collected by this collector. */
	public FullDocs<EXTERNAL> fullDocs() {
		return new LazyFullDocs<>(mapResults(this.pq.reverse()), new TotalHits(totalHits, totalHitsRelation));
	}

	public abstract ResourceIterable<EXTERNAL> mapResults(ResourceIterable<INTERNAL> it);

	@Override
	public void onClose() {
		pq.close();
	}
}