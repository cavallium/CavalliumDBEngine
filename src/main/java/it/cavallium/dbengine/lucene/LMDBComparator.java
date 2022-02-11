package it.cavallium.dbengine.lucene;

import static org.apache.lucene.search.SortField.STRING_LAST;

import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.comparators.DoubleComparator;
import it.cavallium.dbengine.lucene.comparators.FloatComparator;
import it.cavallium.dbengine.lucene.comparators.IntComparator;
import it.cavallium.dbengine.lucene.comparators.LongComparator;
import it.cavallium.dbengine.lucene.comparators.RelevanceComparator;
import it.cavallium.dbengine.lucene.comparators.TermOrdValComparator;
import java.io.IOException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.comparators.LMDBDocComparator;

public class LMDBComparator {

	public static FieldComparator<?> getComparator(LLTempLMDBEnv env, SortField sortField,
			int numHits, int sortPos) {
		var sortFieldClass = sortField.getClass();
		if (sortFieldClass == org.apache.lucene.search.SortedNumericSortField.class) {
			var nf = (org.apache.lucene.search.SortedNumericSortField) sortField;
			var type = nf.getNumericType();
			var missingValue = nf.getMissingValue();
			var reverse = nf.getReverse();
			var selector = nf.getSelector();
			final FieldComparator<?> fieldComparator = switch (type) {
				case INT -> new IntComparator(env, numHits, nf.getField(), (Integer) missingValue, reverse, sortPos) {
					@Override
					public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
						return new IntLeafComparator(context) {
							@Override
							protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field)
									throws IOException {
								return SortedNumericSelector.wrap(DocValues.getSortedNumeric(context.reader(), field), selector, type);
							}
						};
					}
				};
				case FLOAT -> new FloatComparator(env, numHits, nf.getField(), (Float) missingValue, reverse, sortPos) {
					@Override
					public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
						return new FloatLeafComparator(context) {
							@Override
							protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field)
									throws IOException {
								return SortedNumericSelector.wrap(DocValues.getSortedNumeric(context.reader(), field), selector, type);
							}
						};
					}
				};
				case LONG -> new LongComparator(env, numHits, nf.getField(), (Long) missingValue, reverse, sortPos) {
					@Override
					public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
						return new LongLeafComparator(context) {
							@Override
							protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field)
									throws IOException {
								return SortedNumericSelector.wrap(DocValues.getSortedNumeric(context.reader(), field), selector, type);
							}
						};
					}
				};
				case DOUBLE -> new DoubleComparator(env, numHits, nf.getField(), (Double) missingValue, reverse, sortPos) {
					@Override
					public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
						return new DoubleLeafComparator(context) {
							@Override
							protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field)
									throws IOException {
								return SortedNumericSelector.wrap(DocValues.getSortedNumeric(context.reader(), field), selector, type);
							}
						};
					}
				};
				case CUSTOM, DOC, REWRITEABLE, STRING_VAL, SCORE, STRING -> throw new AssertionError();
			};
			if (!nf.getOptimizeSortWithPoints()) {
				fieldComparator.disableSkipping();
			}
			return fieldComparator;
		} else if (sortFieldClass == SortField.class) {
			var missingValue = sortField.getMissingValue();
			var reverse = sortField.getReverse();
			var field = sortField.getField();
			var comparatorSource = sortField.getComparatorSource();
			return switch (sortField.getType()) {
				case SCORE -> new RelevanceComparator(env, numHits);
				case DOC -> new LMDBDocComparator(env, numHits, reverse, sortPos);
				case INT -> new IntComparator(env, numHits, field, (Integer) missingValue,
						reverse, sortPos);
				case FLOAT -> new FloatComparator(env, numHits, field, (Float) missingValue,
						reverse, sortPos);
				case LONG -> new LongComparator(env, numHits, field, (Long) missingValue,
						reverse, sortPos);
				case DOUBLE -> new DoubleComparator(env, numHits, field, (Double) missingValue,
						reverse, sortPos);
				case CUSTOM -> {
					assert comparatorSource != null;
					yield comparatorSource.newComparator(field, numHits, sortPos, reverse);
				}
				case STRING -> new TermOrdValComparator(env, numHits, field, missingValue == STRING_LAST);
				case STRING_VAL -> throw new NotImplementedException("String val sort field not implemented");
				case REWRITEABLE -> throw new IllegalStateException(
						"SortField needs to be rewritten through Sort.rewrite(..) and SortField.rewrite(..)");
			};
		} else {
			throw new NotImplementedException("SortField type not implemented: " + sortFieldClass.getName());
		}
	}
}
