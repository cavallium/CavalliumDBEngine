package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataSerializer;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class LuceneHacksSerializer implements DataSerializer<LuceneHacks> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull LuceneHacks luceneHacks) {
		if (luceneHacks.customLocalSearcher() != null || luceneHacks.customMultiSearcher() != null) {
			throw new UnsupportedOperationException("Can't encode this type");
		}
	}

	@Override
	public @NotNull LuceneHacks deserialize(SafeDataInput dataInput) {
		return new LuceneHacks(null, null);
	}
}
