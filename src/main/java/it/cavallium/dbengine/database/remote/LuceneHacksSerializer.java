package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import it.cavallium.dbengine.lucene.LuceneHacks;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class LuceneHacksSerializer implements DataSerializer<LuceneHacks> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull LuceneHacks luceneHacks) throws IOException {
		if (luceneHacks.customLocalSearcher() != null || luceneHacks.customMultiSearcher() != null) {
			throw new UnsupportedOperationException("Can't encode this type");
		}
	}

	@Override
	public @NotNull LuceneHacks deserialize(DataInput dataInput) throws IOException {
		return new LuceneHacks(null, null);
	}
}
