package it.cavallium.dbengine.lucene;

import static it.cavallium.dbengine.lucene.LLDocElementScoreComparator.SCORE_DOC_SCORE_ELEM_COMPARATOR;
import static it.cavallium.dbengine.utils.StreamUtils.mergeComparing;
import static org.apache.lucene.search.TotalHits.Relation.EQUAL_TO;
import static org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;

import it.cavallium.dbengine.lucene.collector.FullFieldDocs;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.jetbrains.annotations.Nullable;

public interface FullDocs<T extends LLDoc> extends ResourceIterable<T> {

	Comparator<LLDoc> SHARD_INDEX_TIE_BREAKER = Comparator.comparingInt(LLDoc::shardIndex);
	Comparator<LLDoc> DOC_ID_TIE_BREAKER = Comparator.comparingInt(LLDoc::doc);
	Comparator<LLDoc> DEFAULT_TIE_BREAKER = SHARD_INDEX_TIE_BREAKER.thenComparing(DOC_ID_TIE_BREAKER);

	@Override
	Stream<T> iterate();

	@Override
	Stream<T> iterate(long skips);

	TotalHits totalHits();

	static <T extends LLDoc> FullDocs<T> merge(@Nullable Sort sort, FullDocs<T>[] fullDocs) {
		ResourceIterable<T> mergedIterable = mergeResourceIterable(sort, fullDocs);
		TotalHits mergedTotalHits = mergeTotalHits(fullDocs);
		FullDocs<T> docs = new MergedFullDocs<>(mergedIterable, mergedTotalHits);
		if (sort != null) {
			return new FullFieldDocs<>(docs, sort.getSort());
		} else {
			return docs;
		}
	}

	static <T extends LLDoc> int tieBreakCompare(
			T firstDoc,
			T secondDoc,
			Comparator<T> tieBreaker) {
		assert tieBreaker != null;

		int value = tieBreaker.compare(firstDoc, secondDoc);
		if (value == 0) {
			throw new IllegalStateException();
		} else {
			return value;
		}
	}

	static <T extends LLDoc> ResourceIterable<T> mergeResourceIterable(
			@Nullable Sort sort,
			FullDocs<T>[] fullDocs) {
		return new MergedResourceIterable<>(fullDocs, sort);
	}

	static <T extends LLDoc> TotalHits mergeTotalHits(FullDocs<T>[] fullDocs) {
		long totalCount = 0;
		Relation totalRelation = EQUAL_TO;
		for (FullDocs<T> fullDoc : fullDocs) {
			var totalHits = fullDoc.totalHits();
			totalCount += totalHits.value;
			totalRelation = switch (totalHits.relation) {
				case EQUAL_TO -> totalRelation;
				case GREATER_THAN_OR_EQUAL_TO -> totalRelation == EQUAL_TO ? GREATER_THAN_OR_EQUAL_TO : totalRelation;
			};
		}
		return new TotalHits(totalCount, totalRelation);
	}

	class MergedResourceIterable<T extends LLDoc> extends SimpleResource implements ResourceIterable<T> {

		private final FullDocs<T>[] fullDocs;
		private final @Nullable Sort sort;

		public MergedResourceIterable(FullDocs<T>[] fullDocs, @Nullable Sort sort) {
			this.fullDocs = fullDocs;
			this.sort = sort;
		}

		@Override
		protected void onClose() {
			for (FullDocs<T> fullDoc : fullDocs) {
				fullDoc.close();
			}
		}

		@Override
		public Stream<T> iterate() {
			@SuppressWarnings("unchecked") Stream<T>[] iterables = new Stream[fullDocs.length];

			for (int i = 0; i < fullDocs.length; i++) {
				var singleFullDocs = fullDocs[i].iterate();
				iterables[i] = singleFullDocs;
			}

			Comparator<LLDoc> comp;
			if (sort == null) {
				// Merge maintaining sorting order (Algorithm taken from TopDocs.ScoreMergeSortQueue)

				comp = SCORE_DOC_SCORE_ELEM_COMPARATOR.thenComparing(DEFAULT_TIE_BREAKER);
			} else {
				// Merge maintaining sorting order (Algorithm taken from TopDocs.MergeSortQueue)

				SortField[] sortFields = sort.getSort();
				var comparators = new FieldComparator[sortFields.length];
				var reverseMul = new int[sortFields.length];

				for (int compIDX = 0; compIDX < sortFields.length; ++compIDX) {
					SortField sortField = sortFields[compIDX];
					comparators[compIDX] = sortField.getComparator(1, Pruning.NONE);
					reverseMul[compIDX] = sortField.getReverse() ? -1 : 1;
				}

				comp = (first, second) -> {
					assert first != second;

					LLFieldDoc firstFD = (LLFieldDoc) first;
					LLFieldDoc secondFD = (LLFieldDoc) second;

					for (int compIDX = 0; compIDX < comparators.length; ++compIDX) {
						//noinspection rawtypes
						FieldComparator fieldComp = comparators[compIDX];
						//noinspection unchecked
						int cmp = reverseMul[compIDX] * fieldComp.compareValues(firstFD.fields().get(compIDX),
								secondFD.fields().get(compIDX)
						);
						if (cmp != 0) {
							return cmp;
						}
					}

					return tieBreakCompare(first, second, DEFAULT_TIE_BREAKER);
				};
			}

			@SuppressWarnings("unchecked") Stream<T>[] fluxes = new Stream[fullDocs.length];
			for (int i = 0; i < iterables.length; i++) {
				var shardIndex = i;
				fluxes[i] = iterables[i].map(shard -> {
					if (shard instanceof LLScoreDoc scoreDoc) {
						//noinspection unchecked
						return (T) new LLScoreDoc(scoreDoc.doc(), scoreDoc.score(), shardIndex);
					} else if (shard instanceof LLFieldDoc fieldDoc) {
						//noinspection unchecked
						return (T) new LLFieldDoc(fieldDoc.doc(), fieldDoc.score(), shardIndex, fieldDoc.fields());
					} else if (shard instanceof LLSlotDoc slotDoc) {
						//noinspection unchecked
						return (T) new LLSlotDoc(slotDoc.doc(), slotDoc.score(), shardIndex, slotDoc.slot());
					} else {
						throw new UnsupportedOperationException("Unsupported type " + (shard == null ? null : shard.getClass()));
					}
				});
				if (fullDocs[i].totalHits().relation == EQUAL_TO) {
					fluxes[i] = fluxes[i].limit(fullDocs[i].totalHits().value);
				}
			}

			return mergeComparing(comp, fluxes);
		}
	}

	class MergedFullDocs<T extends LLDoc> extends SimpleResource implements FullDocs<T> {

		private final ResourceIterable<T> mergedIterable;
		private final TotalHits mergedTotalHits;

		public MergedFullDocs(ResourceIterable<T> mergedIterable, TotalHits mergedTotalHits) {
			this.mergedIterable = mergedIterable;
			this.mergedTotalHits = mergedTotalHits;
		}

		@Override
		public void onClose() {
			mergedIterable.close();
		}

		@Override
		public Stream<T> iterate() {
			return mergedIterable.iterate();
		}

		@Override
		public Stream<T> iterate(long skips) {
			return mergedIterable.iterate(skips);
		}

		@Override
		public TotalHits totalHits() {
			return mergedTotalHits;
		}
	}
}
