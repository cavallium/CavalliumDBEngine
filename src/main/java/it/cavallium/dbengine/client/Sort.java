package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.BaseType;
import it.cavallium.dbengine.client.query.current.data.DocSort;
import it.cavallium.dbengine.client.query.current.data.NoSort;
import it.cavallium.dbengine.client.query.current.data.NumericSort;
import it.cavallium.dbengine.client.query.current.data.RandomSort;
import it.cavallium.dbengine.client.query.current.data.ScoreSort;
import org.jetbrains.annotations.NotNull;

public record Sort(@NotNull it.cavallium.dbengine.client.query.current.data.Sort querySort) {

	public boolean isSorted() {
		return querySort.getBaseType$() != BaseType.NoSort;
	}

	public static Sort random() {
		return new Sort(RandomSort.of());
	}

	public static Sort score() {
		return new Sort(ScoreSort.of());
	}

	public static Sort no() {
		return new Sort(NoSort.of());
	}

	public static Sort doc() {
		return new Sort(DocSort.of());
	}

	public static Sort numeric(String field, boolean reverse) {
		return new Sort(NumericSort.of(field, reverse));
	}

	@Override
	public String toString() {
		return querySort.toString();
	}
}
