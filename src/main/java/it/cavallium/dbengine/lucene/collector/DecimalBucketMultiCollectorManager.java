package it.cavallium.dbengine.lucene.collector;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.RandomSamplingFacetsCollector;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.DoubleRangeFacetCounts;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.LongRangeFacetCounts;
import org.apache.lucene.facet.range.Range;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.jetbrains.annotations.Nullable;

public class DecimalBucketMultiCollectorManager implements CollectorMultiManager<Buckets, Buckets> {

	private final FacetsCollectorManager facetsCollectorManager;
	private final Range[] bucketRanges;

	private final List<Query> queries;
	private final @Nullable Query normalizationQuery;
	private final @Nullable Integer sampleSize;

	private final String bucketField;
	private final BucketValueSource bucketValueSource;

	private final double totalLength;
	private final double bucketLength;
	private final double minimum;
	private final double maximum;
	private final int buckets;

	// todo: replace with an argument
	private static final boolean USE_LONGS = true;

	public DecimalBucketMultiCollectorManager(double minimum,
			double maximum,
			double buckets,
			String bucketField,
			BucketValueSource bucketValueSource,
			List<Query> queries,
			@Nullable Query normalizationQuery,
			@Nullable Integer sampleSize) {
		this.queries = queries;
		this.normalizationQuery = normalizationQuery;
		var bucketsInt = (int) Math.ceil(buckets);
		this.minimum = minimum;
		this.maximum = maximum;
		this.buckets = bucketsInt;
		this.bucketLength = (maximum - minimum) / bucketsInt;
		this.totalLength = bucketLength * bucketsInt;
		this.bucketField = bucketField;
		this.bucketValueSource = bucketValueSource;
		this.sampleSize = sampleSize;

		if (USE_LONGS) {
			this.bucketRanges = new LongRange[bucketsInt];
		} else {
			this.bucketRanges = new DoubleRange[bucketsInt];
		}
		for (int i = 0; i < bucketsInt; i++) {
			double offsetMin = minimum + (bucketLength * i);
			double offsetMax = minimum + (bucketLength * (i + 1));
			if (USE_LONGS) {
				this.bucketRanges[i] = new LongRange(Integer.toString(i),
						(long) offsetMin,
						true,
						(long) offsetMax,
						i == bucketsInt - 1
				);
			} else {
				this.bucketRanges[i] = new DoubleRange(Integer.toString(i),
						offsetMin,
						true,
						offsetMax,
						i == bucketsInt - 1
				);
			}
		}

		this.facetsCollectorManager = new FacetsCollectorManager() {
			@Override
			public FacetsCollector newCollector() {
				if (sampleSize != null) {
					return new RandomSamplingFacetsCollector(sampleSize) {
						@Override
						public ScoreMode scoreMode() {
							return ScoreMode.COMPLETE_NO_SCORES;
						}
					};
				} else {
					return new FacetsCollector(false) {
						@Override
						public ScoreMode scoreMode() {
							return ScoreMode.COMPLETE_NO_SCORES;
						}
					};
				}
			}
		};
	}

	public double[] newBuckets() {
		return new double[buckets];
	}

	public Buckets search(IndexSearcher indexSearcher) throws IOException {
		Query globalQuery;
		if (normalizationQuery != null) {
			globalQuery = normalizationQuery;
		} else {
			var booleanQueryBuilder = new BooleanQuery.Builder();
			for (Query query : queries) {
				booleanQueryBuilder.add(query, Occur.SHOULD);
			}
			booleanQueryBuilder.setMinimumNumberShouldMatch(1);
			globalQuery = booleanQueryBuilder.build();
		}
		var facetsCollector = indexSearcher.search(globalQuery, facetsCollectorManager);
		double[] reducedNormalizationBuckets = newBuckets();
		List<DoubleArrayList> seriesReducedBuckets = new ArrayList<>(queries.size());
		for (int i = 0; i < queries.size(); i++) {
			var buckets = newBuckets();
			seriesReducedBuckets.add(DoubleArrayList.wrap(buckets));
		}
		int serieIndex = 0;
		for (Query query : queries) {
			var reducedBuckets = seriesReducedBuckets.get(serieIndex);
			Facets facets;
			if (USE_LONGS) {
				LongValuesSource valuesSource;
				if (bucketValueSource instanceof NullValueSource) {

					valuesSource = null;
				} else if (bucketValueSource instanceof ConstantValueSource constantValueSource) {
					valuesSource = LongValuesSource.constant(constantValueSource.constant().longValue());
				} else if (bucketValueSource instanceof LongBucketValueSource longBucketValueSource) {
					valuesSource = longBucketValueSource.source();
				} else {
					throw new IllegalArgumentException("Wrong value source type: " + bucketValueSource);
				}
				facets = new LongRangeFacetCounts(bucketField,
						valuesSource,
						facetsCollector,
						query,
						(LongRange[]) bucketRanges
				);
			} else {
				DoubleValuesSource valuesSource;
				if (bucketValueSource instanceof NullValueSource) {
					valuesSource = null;
				} else if (bucketValueSource instanceof ConstantValueSource constantValueSource) {
					valuesSource = DoubleValuesSource.constant(constantValueSource.constant().longValue());
				} else if (bucketValueSource instanceof DoubleBucketValueSource doubleBucketValueSource) {
					valuesSource = doubleBucketValueSource.source();
				} else {
					throw new IllegalArgumentException("Wrong value source type: " + bucketValueSource);
				}
				facets = new DoubleRangeFacetCounts(bucketField,
						valuesSource,
						facetsCollector,
						query,
						(DoubleRange[]) bucketRanges
				);
			}
			FacetResult children = facets.getTopChildren(0, bucketField);
			for (LabelAndValue labelAndValue : children.labelValues) {
				var index = Integer.parseInt(labelAndValue.label);
				reducedBuckets.set(index, reducedBuckets.getDouble(index) + labelAndValue.value.doubleValue());
			}
			serieIndex++;
		}

		Facets normalizationFacets;
		if (normalizationQuery != null) {
			if (USE_LONGS) {
				LongValuesSource valuesSource;
				if (bucketValueSource instanceof NullValueSource) {
					valuesSource = null;
				} else if (bucketValueSource instanceof ConstantValueSource constantValueSource) {
					valuesSource = LongValuesSource.constant(constantValueSource.constant().longValue());
				} else if (bucketValueSource instanceof LongBucketValueSource longBucketValueSource) {
					valuesSource = longBucketValueSource.source();
				} else {
					throw new IllegalArgumentException("Wrong value source type: " + bucketValueSource);
				}
				normalizationFacets = new LongRangeFacetCounts(bucketField,
						valuesSource,
						facetsCollector,
						null,
						(LongRange[]) bucketRanges
				);
			} else {
				DoubleValuesSource valuesSource;
				if (bucketValueSource instanceof NullValueSource) {
					valuesSource = null;
				} else if (bucketValueSource instanceof ConstantValueSource constantValueSource) {
					valuesSource = DoubleValuesSource.constant(constantValueSource.constant().longValue());
				} else if (bucketValueSource instanceof DoubleBucketValueSource doubleBucketValueSource) {
					valuesSource = doubleBucketValueSource.source();
				} else {
					throw new IllegalArgumentException("Wrong value source type: " + bucketValueSource);
				}
				normalizationFacets = new DoubleRangeFacetCounts(bucketField,
						valuesSource,
						facetsCollector,
						null,
						(DoubleRange[]) bucketRanges
				);
			}
			var normalizationChildren = normalizationFacets.getTopChildren(0, bucketField);
			for (LabelAndValue labelAndValue : normalizationChildren.labelValues) {
				var index = Integer.parseInt(labelAndValue.label);
				reducedNormalizationBuckets[index] += labelAndValue.value.doubleValue();
			}
		} else {
			Arrays.fill(reducedNormalizationBuckets, 1);
		}
		return new Buckets(seriesReducedBuckets,  DoubleArrayList.wrap(reducedNormalizationBuckets));
	}

	@Override
	public ScoreMode scoreMode() {
		throw new NotImplementedException();
	}

	@Override
	public Buckets reduce(List<Buckets> reducedBucketsList) throws IOException {
		List<DoubleArrayList> seriesReducedValues = new ArrayList<>();
		double[] reducedTotals = newBuckets();
		for (var seriesBuckets : reducedBucketsList) {
			for (DoubleArrayList values : seriesBuckets.seriesValues()) {
				double[] reducedValues = newBuckets();
				for (int i = 0; i < values.size(); i++) {
					reducedValues[i] += values.getDouble(i);
				}
				seriesReducedValues.add(DoubleArrayList.wrap(reducedValues));
			}
			var totals = seriesBuckets.totals();
			for (int i = 0; i < totals.size(); i++) {
				reducedTotals[i] += totals.getDouble(i);
			}
		}
		return new Buckets(seriesReducedValues, DoubleArrayList.wrap(reducedTotals));
	}
}
