package it.cavallium.dbengine.lucene.directory;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90Codec;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;

public final class Lucene90CodecWithNoFieldCompression extends FilterCodec {

	private final StoredFieldsFormat storedFieldsFormat;

	public Lucene90CodecWithNoFieldCompression() {
		super("Lucene410CodecWithNoFieldCompression", new Lucene90Codec());
		storedFieldsFormat = new Lucene90NoCompressionStoredFieldsFormat();
	}

	@Override
	public StoredFieldsFormat storedFieldsFormat() {
		return storedFieldsFormat;
	}
}