package it.cavallium.dbengine.lucene.searcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermStatistics;
import org.jetbrains.annotations.Nullable;

public class ShardIndexSearcher extends IndexSearcher {
	public final int myNodeID;
	private final IndexSearcher[] searchers;

	private final Map<FieldAndShar, CachedCollectionStatistics> collectionStatsCache;
	private final Map<TermAndShard, TermStatistics> termStatsCache;

	public ShardIndexSearcher(SharedShardStatistics sharedShardStatistics, List<IndexSearcher> searchers, int nodeID) {
		super(searchers.get(nodeID).getIndexReader(), searchers.get(nodeID).getExecutor());
		this.collectionStatsCache = sharedShardStatistics.collectionStatsCache;
		this.termStatsCache = sharedShardStatistics.termStatsCache;
		this.searchers = searchers.toArray(IndexSearcher[]::new);
		myNodeID = nodeID;
	}

	public static List<IndexSearcher> create(Iterable<IndexSearcher> indexSearchersIterable) {
		var it = indexSearchersIterable.iterator();
		if (it.hasNext()) {
			var is = it.next();
			if (!it.hasNext()) {
				return List.of(is);
			}
			if (is instanceof ShardIndexSearcher) {
				if (indexSearchersIterable instanceof List<IndexSearcher> list) {
					return list;
				} else {
					var result = new ArrayList<IndexSearcher>();
					result.add(is);
					do {
						result.add(it.next());
					} while (it.hasNext());
					return result;
				}
			}
		}
		List<IndexSearcher> indexSearchers;
		if (indexSearchersIterable instanceof List<IndexSearcher> collection) {
			indexSearchers = collection;
		} else if (indexSearchersIterable instanceof Collection<IndexSearcher> collection) {
			indexSearchers = List.copyOf(collection);
		} else {
			indexSearchers = new ArrayList<>();
			for (IndexSearcher i : indexSearchersIterable) {
				indexSearchers.add(i);
			}
		}
		if (indexSearchers.size() == 0) {
			return List.of();
		} else {
			var sharedShardStatistics = new SharedShardStatistics();
			List<IndexSearcher> result = new ArrayList<>(indexSearchers.size());
			for (int nodeId = 0; nodeId < indexSearchers.size(); nodeId++) {
				result.add(new ShardIndexSearcher(sharedShardStatistics, indexSearchers, nodeId));
			}
			return result;
		}
	}

	@Override
	public Query rewrite(Query original) throws IOException {
		final IndexSearcher localSearcher = new IndexSearcher(getIndexReader());
		original = localSearcher.rewrite(original);
		final Set<Term> terms = new HashSet<>();
		original.visit(QueryVisitor.termCollector(terms));

		// Make a single request to remote nodes for term
		// stats:
		for (int nodeID = 0; nodeID < searchers.length; nodeID++) {
			if (nodeID == myNodeID) {
				continue;
			}

			final Set<Term> missing = new HashSet<>();
			for (Term term : terms) {
				final TermAndShard key = new TermAndShard(nodeID, term);
				if (!termStatsCache.containsKey(key)) {
					missing.add(term);
				}
			}
			if (missing.size() != 0) {
				for (Map.Entry<Term, TermStatistics> ent : getNodeTermStats(missing, nodeID).entrySet()) {
					if (ent.getValue() != null) {
						final TermAndShard key = new TermAndShard(nodeID, ent.getKey());
						termStatsCache.put(key, ent.getValue());
					}
				}
			}
		}

		return original;
	}

	// Mock: in a real env, this would hit the wire and get
	// term stats from remote node
	Map<Term, TermStatistics> getNodeTermStats(Set<Term> terms, int nodeID) throws IOException {
		var s = searchers[nodeID];
		final Map<Term, TermStatistics> stats = new HashMap<>();
		if (s == null) {
			throw new NoSuchElementException("node=" + nodeID);
		}
		for (Term term : terms) {
			final TermStates ts = TermStates.build(s, term, true);
			if (ts.docFreq() > 0) {
				stats.put(term, s.termStatistics(term, ts.docFreq(), ts.totalTermFreq()));
			}
		}
		return stats;
	}

	@Override
	public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq)
			throws IOException {
		assert term != null;
		long distributedDocFreq = 0;
		long distributedTotalTermFreq = 0;
		for (int nodeID = 0; nodeID < searchers.length; nodeID++) {

			final TermStatistics subStats;
			if (nodeID == myNodeID) {
				subStats = super.termStatistics(term, docFreq, totalTermFreq);
			} else {
				final TermAndShard key = new TermAndShard(nodeID, term);
				subStats = termStatsCache.get(key);
				if (subStats == null) {
					continue; // term not found
				}
			}

			long nodeDocFreq = subStats.docFreq();
			distributedDocFreq += nodeDocFreq;

			long nodeTotalTermFreq = subStats.totalTermFreq();
			distributedTotalTermFreq += nodeTotalTermFreq;
		}
		assert distributedDocFreq > 0;
		return new TermStatistics(term.bytes(), distributedDocFreq, distributedTotalTermFreq);
	}

	@Override
	public CollectionStatistics collectionStatistics(String field) throws IOException {
		// TODO: we could compute this on init and cache,
		// since we are re-inited whenever any nodes have a
		// new reader
		long docCount = 0;
		long sumTotalTermFreq = 0;
		long sumDocFreq = 0;
		long maxDoc = 0;

		for (int nodeID = 0; nodeID < searchers.length; nodeID++) {
			final FieldAndShar key = new FieldAndShar(nodeID, field);
			final CollectionStatistics nodeStats;
			if (nodeID == myNodeID) {
				nodeStats = super.collectionStatistics(field);
				collectionStatsCache.put(key, new CachedCollectionStatistics(nodeStats));
			} else {
				var nodeStatsOptional = collectionStatsCache.get(key);
				if (nodeStatsOptional == null) {
					nodeStatsOptional = new CachedCollectionStatistics(computeNodeCollectionStatistics(key));
					collectionStatsCache.put(key, nodeStatsOptional);
				}
				nodeStats = nodeStatsOptional.collectionStatistics();
			}
			if (nodeStats == null) {
				continue; // field not in sub at all
			}

			long nodeDocCount = nodeStats.docCount();
			docCount += nodeDocCount;

			long nodeSumTotalTermFreq = nodeStats.sumTotalTermFreq();
			sumTotalTermFreq += nodeSumTotalTermFreq;

			long nodeSumDocFreq = nodeStats.sumDocFreq();
			sumDocFreq += nodeSumDocFreq;

			assert nodeStats.maxDoc() >= 0;
			maxDoc += nodeStats.maxDoc();
		}

		if (maxDoc == 0) {
			return null; // field not found across any node whatsoever
		} else {
			return new CollectionStatistics(field, maxDoc, docCount, sumTotalTermFreq, sumDocFreq);
		}
	}

	private CollectionStatistics computeNodeCollectionStatistics(FieldAndShar fieldAndShard) throws IOException {
		var searcher = searchers[fieldAndShard.nodeID];
		return searcher.collectionStatistics(fieldAndShard.field);
	}

	public record CachedCollectionStatistics(@Nullable CollectionStatistics collectionStatistics) {}

	public static class TermAndShard {
		private final int nodeID;
		private final Term term;

		public TermAndShard(int nodeID, Term term) {
			this.nodeID = nodeID;
			this.term = term;
		}

		@Override
		public int hashCode() {
			return (nodeID + term.hashCode());
		}

		@Override
		public boolean equals(Object _other) {
			if (!(_other instanceof final TermAndShard other)) {
				return false;
			}

			return term.equals(other.term) && nodeID == other.nodeID;
		}
	}


	public static class FieldAndShar {
		private final int nodeID;
		private final String field;

		public FieldAndShar(int nodeID, String field) {
			this.nodeID = nodeID;
			this.field = field;
		}

		@Override
		public int hashCode() {
			return (nodeID + field.hashCode());
		}

		@Override
		public boolean equals(Object _other) {
			if (!(_other instanceof final FieldAndShar other)) {
				return false;
			}

			return field.equals(other.field) && nodeID == other.nodeID;
		}

		@Override
		public String toString() {
			return "FieldAndShardVersion(field="
					+ field
					+ " nodeID="
					+ nodeID
					+ ")";
		}
	}
}