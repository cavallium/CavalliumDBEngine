package it.cavallium.dbengine.database.serialization;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;

@NotAtomic
public class Codecs<A> {

	private final Int2ObjectMap<Codec<A>> codecs;

	public Codecs() {
		codecs = new Int2ObjectOpenHashMap<>();
	}

	public void registerCodec(int id, Codec<A> serializer) {
		if (codecs.put(id, serializer) != null) {
			throw new IllegalArgumentException("Codec " + id + " already registered!");
		}
	}

	public Codec<A> getCodec(int id) {
		var codec = codecs.get(id);
		if (codec == null) {
			throw new UnsupportedOperationException("Unsupported codec " + id);
		}
		return codec;
	}
}
