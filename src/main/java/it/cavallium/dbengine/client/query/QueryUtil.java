package it.cavallium.dbengine.client.query;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;

public class QueryUtil {

	@SuppressWarnings("unused")
	public static String toHumanReadableString(TotalHitsCount totalHitsCount) {
		if (totalHitsCount.exact()) {
			return Long.toString(totalHitsCount.value());
		} else {
			return totalHitsCount.value() + "+";
		}
	}

}
