package it.cavallium.dbengine.lucene.collector;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record Buckets(List<DoubleArrayList> seriesValues, DoubleArrayList totals) {

	public Buckets {
		for (DoubleArrayList values : seriesValues) {
			if (values.size() != totals.size()) {
				throw new IllegalArgumentException("Buckets size mismatch");
			}
		}
	}

	public List<DoubleArrayList> normalized() {
		var normalizedSeries = new ArrayList<DoubleArrayList>(seriesValues.size());
		for (DoubleArrayList values : seriesValues) {
			DoubleArrayList normalized = new DoubleArrayList(values.size());
			for (int i = 0; i < values.size(); i++) {
				normalized.add(values.getDouble(i) / totals.getDouble(i));
			}
			normalizedSeries.add(normalized);
		}
		return normalizedSeries;
	}
}
