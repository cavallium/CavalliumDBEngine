package it.cavallium.dbengine.lucene.collector;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.RandomSamplingFacetsCollector;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.DoubleRangeFacetCounts;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.LongRangeFacetCounts;
import org.apache.lucene.facet.range.Range;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingCollector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.jetbrains.annotations.Nullable;

public class DecimalBucketMultiCollectorManager implements CollectorMultiManager<Buckets, Buckets> {

	private static final boolean USE_SINGLE_FACET_COLLECTOR = true;
	private static final boolean AMORTIZE = true;
	private final boolean randomSamplingEnabled;
	private final FastFacetsCollectorManager facetsCollectorManager;
	private final FastRandomSamplingFacetsCollector randomSamplingFacetsCollector;
	private final Range[] bucketRanges;

	private final List<Query> queries;
	private final @Nullable Query normalizationQuery;
	private final @Nullable Integer collectionRate;
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
			@Nullable Integer collectionRate,
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
		this.collectionRate = collectionRate;
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

		this.randomSamplingEnabled = sampleSize != null;
		int intCollectionRate = this.collectionRate == null ? 1 : this.collectionRate;
		if (randomSamplingEnabled) {
			randomSamplingFacetsCollector = new FastRandomSamplingFacetsCollector(intCollectionRate, sampleSize, 0);
			this.facetsCollectorManager = null;
		} else {
			this.randomSamplingFacetsCollector = null;
			this.facetsCollectorManager = new FastFacetsCollectorManager(intCollectionRate);
		}
	}

	public double[] newBuckets() {
		return new double[buckets];
	}

	public Buckets search(IndexSearcher indexSearcher) throws IOException {
		Query query;
		if (USE_SINGLE_FACET_COLLECTOR && normalizationQuery != null) {
			query = normalizationQuery;
		} else if (queries.size() == 0) {
			query = new MatchNoDocsQuery();
		} else if (queries.size() == 1) {
			query = queries.get(0);
		} else {
			var booleanQueryBuilder = new BooleanQuery.Builder();
			for (Query queryEntry : queries) {
				booleanQueryBuilder.add(queryEntry, Occur.SHOULD);
			}
			booleanQueryBuilder.setMinimumNumberShouldMatch(1);
			query = booleanQueryBuilder.build();
		}
		it.cavallium.dbengine.lucene.collector.FacetsCollector queryFacetsCollector;
		if (randomSamplingEnabled) {
			indexSearcher.search(query, randomSamplingFacetsCollector);
			queryFacetsCollector = randomSamplingFacetsCollector;
		} else {
			queryFacetsCollector = indexSearcher.search(query, facetsCollectorManager);
		}
		double[] reducedNormalizationBuckets = newBuckets();
		List<DoubleArrayList> seriesReducedBuckets = new ArrayList<>(queries.size());
		for (int i = 0; i < queries.size(); i++) {
			var buckets = newBuckets();
			seriesReducedBuckets.add(DoubleArrayList.wrap(buckets));
		}
		int serieIndex = 0;
		for (Query queryEntry : queries) {
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
						queryFacetsCollector.getLuceneFacetsCollector(),
						USE_SINGLE_FACET_COLLECTOR && normalizationQuery != null || queries.size() > 1 ? queryEntry : null,
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
						queryFacetsCollector.getLuceneFacetsCollector(),
						USE_SINGLE_FACET_COLLECTOR && normalizationQuery != null || queries.size() > 1 ? queryEntry : null,
						(DoubleRange[]) bucketRanges
				);
			}
			FacetResult children = facets.getTopChildren(0, bucketField);
			if (AMORTIZE && randomSamplingEnabled) {
				var cfg = new FacetsConfig();
				for (Range bucketRange : bucketRanges) {
					cfg.setIndexFieldName(bucketRange.label, bucketField);
				}
				((RandomSamplingFacetsCollector) queryFacetsCollector.getLuceneFacetsCollector()).amortizeFacetCounts(children, cfg, indexSearcher);
			}
			for (LabelAndValue labelAndValue : children.labelValues) {
				var index = Integer.parseInt(labelAndValue.label);
				reducedBuckets.set(index, reducedBuckets.getDouble(index) + labelAndValue.value.doubleValue());
			}
			serieIndex++;
		}

		it.cavallium.dbengine.lucene.collector.FacetsCollector normalizationFacetsCollector;
		Facets normalizationFacets;
		if (normalizationQuery != null) {
			if (USE_SINGLE_FACET_COLLECTOR) {
				normalizationFacetsCollector = queryFacetsCollector;
			} else if (randomSamplingEnabled) {
				indexSearcher.search(normalizationQuery, randomSamplingFacetsCollector);
				normalizationFacetsCollector = randomSamplingFacetsCollector;
			} else {
				normalizationFacetsCollector = indexSearcher.search(normalizationQuery, facetsCollectorManager);
			}
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
						normalizationFacetsCollector.getLuceneFacetsCollector(),
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
						normalizationFacetsCollector.getLuceneFacetsCollector(),
						null,
						(DoubleRange[]) bucketRanges
				);
			}
			var normalizationChildren = normalizationFacets.getTopChildren(0, bucketField);
			if (AMORTIZE && randomSamplingEnabled) {
				var cfg = new FacetsConfig();
				for (Range bucketRange : bucketRanges) {
					cfg.setIndexFieldName(bucketRange.label, bucketField);
				}
				((RandomSamplingFacetsCollector) normalizationFacetsCollector.getLuceneFacetsCollector()).amortizeFacetCounts(normalizationChildren, cfg, indexSearcher);
			}
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
