package it.cavallium.dbengine.lucene.collector;

import it.cavallium.dbengine.lucene.LuceneUtils;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class DecimalBucketCollectorMultiManager implements CollectorMultiManager<DoubleArrayList, DoubleArrayList> {


	private final String bucketField;
	@Nullable
	private final String valueField;
	private final Set<String> fieldsToLoad;

	private final double totalLength;
	private final double bucketLength;
	private final double minimum;
	private final double maximum;
	private final int buckets;

	public DecimalBucketCollectorMultiManager(double minimum,
			double maximum,
			double buckets,
			String bucketField,
			@Nullable String valueField) {
		var bucketsInt = (int) Math.ceil(buckets);
		this.minimum = minimum;
		this.maximum = maximum;
		this.buckets = bucketsInt;
		this.bucketLength = (maximum - minimum) / bucketsInt;
		this.totalLength = bucketLength * bucketsInt;
		this.bucketField = bucketField;
		this.valueField = valueField;
		if (valueField != null) {
			this.fieldsToLoad = Set.of(bucketField, valueField);
		} else {
			this.fieldsToLoad = Set.of(bucketField);
		}
	}

	public double[] newBuckets() {
		return new double[buckets];
	}

	public CollectorManager<BucketsCollector, DoubleArrayList> get(IndexSearcher indexSearcher) {

		return new CollectorManager<>() {
			@Override
			public BucketsCollector newCollector() {
				return new BucketsCollector(indexSearcher);
			}

			@Override
			public DoubleArrayList reduce(Collection<BucketsCollector> collectors) {
				double[] reducedBuckets = newBuckets();
				for (BucketsCollector collector : collectors) {
					var buckets = collector.getBuckets();
					assert reducedBuckets.length == buckets.length;
					for (int i = 0; i < buckets.length; i++) {
						reducedBuckets[i] += buckets[i];
					}
				}
				return DoubleArrayList.wrap(reducedBuckets);
			}
		};
	}

	@Override
	public ScoreMode scoreMode() {
		throw new NotImplementedException();
	}

	@Override
	public DoubleArrayList reduce(List<DoubleArrayList> reducedBucketsList) {
		if (reducedBucketsList.size() == 1) {
			return reducedBucketsList.get(0);
		}
		double[] reducedBuckets = newBuckets();
		for (DoubleArrayList buckets : reducedBucketsList) {
			for (int i = 0; i < buckets.size(); i++) {
				reducedBuckets[i] += buckets.getDouble(i);
			}
		}
		return DoubleArrayList.wrap(reducedBuckets);
	}

	private class BucketsCollector extends SimpleCollector {

		private final IndexSearcher indexSearcher;
		private final DocumentStoredFieldVisitor documentStoredFieldVisitor;
		private final double[] buckets;

		public BucketsCollector(IndexSearcher indexSearcher) {
			super();
			this.indexSearcher = indexSearcher;
			this.documentStoredFieldVisitor = new DocumentStoredFieldVisitor(fieldsToLoad);
			this.buckets = newBuckets();
		}


		@Override
		public ScoreMode scoreMode() {
			return ScoreMode.COMPLETE_NO_SCORES;
		}

		@Override
		public void collect(int doc) throws IOException {
			indexSearcher.doc(doc, documentStoredFieldVisitor);
			var document = documentStoredFieldVisitor.getDocument();
			var bucketField = document.getField(DecimalBucketCollectorMultiManager.this.bucketField);
			IndexableField valueField;
			if (DecimalBucketCollectorMultiManager.this.valueField != null) {
				valueField = document.getField(DecimalBucketCollectorMultiManager.this.valueField);
			} else {
				valueField = null;
			}
			var bucketValue = bucketField.numericValue().doubleValue();
			if (bucketValue >= minimum && bucketValue <= maximum) {
				double value;
				if (valueField != null) {
					value = valueField.numericValue().doubleValue();
				} else {
					value = 1.0d;
				}
				double bucketIndex = (bucketValue - minimum) / bucketLength;
				int bucketIndexLow = (int) Math.floor(bucketIndex);
				double ratio = (bucketIndex - bucketIndexLow);
				assert ratio >= 0 && ratio <= 1;
				double loValue = value * (1d - ratio);
				double hiValue = value * ratio;
				buckets[bucketIndexLow] += loValue;
				if (hiValue > 0d) {
					buckets[bucketIndexLow + 1] += hiValue;
				}
			}
		}

		public double[] getBuckets() {
			return buckets;
		}
	}
}
