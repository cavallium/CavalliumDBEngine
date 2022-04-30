package it.cavallium.dbengine.lucene;

import static org.apache.lucene.search.SortField.STRING_LAST;

import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
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
import it.cavallium.dbengine.lucene.hugepq.search.comparators.HugePqDocComparator;

public class HugePqComparator {

	public static FieldComparator<?> getComparator(LLTempHugePqEnv env, SortField sortField,
			int numHits, boolean enableSkipping) {
		var sortFieldClass = sortField.getClass();
		if (sortFieldClass == org.apache.lucene.search.SortedNumericSortField.class) {
			var nf = (org.apache.lucene.search.SortedNumericSortField) sortField;
			var type = nf.getNumericType();
			var missingValue = nf.getMissingValue();
			var reverse = nf.getReverse();
			var selector = nf.getSelector();
			final FieldComparator<?> fieldComparator = switch (type) {
				case INT -> new IntComparator(env, numHits, nf.getField(), (Integer) missingValue, reverse, enableSkipping) {
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
				case FLOAT -> new FloatComparator(env, numHits, nf.getField(), (Float) missingValue, reverse, enableSkipping) {
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
				case LONG -> new LongComparator(env, numHits, nf.getField(), (Long) missingValue, reverse, enableSkipping) {
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
				case DOUBLE -> new DoubleComparator(env, numHits, nf.getField(), (Double) missingValue, reverse, enableSkipping) {
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
				case DOC -> new HugePqDocComparator(env, numHits, reverse, enableSkipping);
				case INT -> new IntComparator(env, numHits, field, (Integer) missingValue,
						reverse, enableSkipping);
				case FLOAT -> new FloatComparator(env, numHits, field, (Float) missingValue,
						reverse, enableSkipping);
				case LONG -> new LongComparator(env, numHits, field, (Long) missingValue,
						reverse, enableSkipping);
				case DOUBLE -> new DoubleComparator(env, numHits, field, (Double) missingValue,
						reverse, enableSkipping);
				case CUSTOM -> {
					assert comparatorSource != null;
					yield comparatorSource.newComparator(field, numHits, enableSkipping, reverse);
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
