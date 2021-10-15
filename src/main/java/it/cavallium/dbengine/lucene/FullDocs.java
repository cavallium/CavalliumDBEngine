package it.cavallium.dbengine.lucene;

import static it.cavallium.dbengine.lucene.LLDocElementScoreComparator.SCORE_DOC_SCORE_ELEM_COMPARATOR;
import static org.apache.lucene.search.TotalHits.Relation.*;

import it.cavallium.dbengine.lucene.collector.FullFieldDocs;
import java.util.Comparator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;

public interface FullDocs<T extends LLDoc> extends ResourceIterable<T> {

	Comparator<LLDoc> SHARD_INDEX_TIE_BREAKER = Comparator.comparingInt(LLDoc::shardIndex);
	Comparator<LLDoc> DOC_ID_TIE_BREAKER = Comparator.comparingInt(LLDoc::doc);
	Comparator<LLDoc> DEFAULT_TIE_BREAKER = SHARD_INDEX_TIE_BREAKER.thenComparing(DOC_ID_TIE_BREAKER);

	@Override
	Flux<T> iterate();

	@Override
	Flux<T> iterate(long skips);

	TotalHits totalHits();

	static <T extends LLDoc> FullDocs<T> merge(@Nullable Sort sort, FullDocs<T>[] fullDocs) {
		ResourceIterable<T> mergedIterable = mergeResourceIterable(sort, fullDocs);
		TotalHits mergedTotalHits = mergeTotalHits(fullDocs);
		FullDocs<T> docs = new FullDocs<>() {
			@Override
			public Flux<T> iterate() {
				return mergedIterable.iterate();
			}

			@Override
			public Flux<T> iterate(long skips) {
				return mergedIterable.iterate(skips);
			}

			@Override
			public TotalHits totalHits() {
				return mergedTotalHits;
			}
		};
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
		return () -> {
			@SuppressWarnings("unchecked")
			Flux<T>[] iterables = new Flux[fullDocs.length];

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

				for(int compIDX = 0; compIDX < sortFields.length; ++compIDX) {
					SortField sortField = sortFields[compIDX];
					comparators[compIDX] = sortField.getComparator(1, compIDX);
					reverseMul[compIDX] = sortField.getReverse() ? -1 : 1;
				}

				comp = (first, second) -> {
					assert first != second;

					LLFieldDoc firstFD = (LLFieldDoc) first;
					LLFieldDoc secondFD = (LLFieldDoc) second;

					for(int compIDX = 0; compIDX < comparators.length; ++compIDX) {
						//noinspection rawtypes
						FieldComparator fieldComp = comparators[compIDX];
						//noinspection unchecked
						int cmp = reverseMul[compIDX] * fieldComp.compareValues(firstFD.fields().get(compIDX), secondFD.fields().get(compIDX));
						if (cmp != 0) {
							return cmp;
						}
					}

					return tieBreakCompare(first, second, DEFAULT_TIE_BREAKER);
				};
			}

			@SuppressWarnings("unchecked")
			Flux<T>[] fluxes = new Flux[fullDocs.length];
			for (int i = 0; i < iterables.length; i++) {
				var shardIndex = i;
				fluxes[i] = iterables[i].<T>map(shard -> {
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
						throw new UnsupportedOperationException("Unsupported type " + shard.getClass());
					}
				});
				if (fullDocs[i].totalHits().relation == EQUAL_TO) {
					fluxes[i] = fluxes[i].take(fullDocs[i].totalHits().value, true);
				}
			}

			return Flux.mergeComparing(comp, fluxes);
		};
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
}
