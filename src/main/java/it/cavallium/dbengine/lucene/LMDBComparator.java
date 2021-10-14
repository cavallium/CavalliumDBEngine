package it.cavallium.dbengine.lucene;

import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import java.util.Comparator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;

public class LMDBComparator {

	public static FieldComparator<?> getComparator(LLTempLMDBEnv env, SortField field, int sortPos) {
		throw new UnsupportedOperationException("not implemented");
	}
}
