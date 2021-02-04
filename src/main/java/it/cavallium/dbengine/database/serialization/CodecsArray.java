package it.cavallium.dbengine.database.serialization;

import org.warp.commonutils.concurrency.atomicity.NotAtomic;

@NotAtomic
public class CodecsArray<A> implements Codecs<A> {

	private final Codec<A>[] codecs;

	public CodecsArray() {
		//noinspection unchecked
		codecs = new Codec[256];
	}

	@Override
	public synchronized void registerCodec(int id, Codec<A> serializer) {
		if (codecs[id] != null) {
			throw new IllegalArgumentException("Codec " + id + " already registered!");
		}
		codecs[id] = serializer;
	}

	@Override
	public Codec<A> getCodec(int id) {
		var codec = codecs[id];
		if (codec == null) {
			throw new UnsupportedOperationException("Unsupported codec " + id);
		}
		return codec;
	}
}
