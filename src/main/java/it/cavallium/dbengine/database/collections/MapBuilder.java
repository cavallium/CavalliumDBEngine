package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.database.LLDictionary;

public class MapBuilder {

	private final LLDictionary dictionary;

	public MapBuilder(LLDictionary dictionary) {
		this.dictionary = dictionary;
	}

	public static MapBuilder of(LLDictionary dictionary) {
		return new MapBuilder(dictionary);
	}

	public MapBuilder2<byte[], byte[]> map() {
		return MapBuilder2.fromDictionary(dictionary);
	}

	public static class MapBuilder2<K, V> {

		private final LLDictionary dictionary;
		private final MapBuilder2<?, ?> parent;
		private final SerializationOptions<?, K, ?, V> serializationOptions;

		public MapBuilder2(LLDictionary dictionary, SerializationOptions<byte[], K, byte[], V> serializationOptions) {
			this.dictionary = dictionary;
			this.parent = null;
			this.serializationOptions = serializationOptions;
		}

		private <K1, V1> MapBuilder2(MapBuilder2<K1, V1> parent, SerializationOptions<K1, K, V1, V> serializationOptions) {
			this.dictionary = null;
			this.parent = parent;
			this.serializationOptions = serializationOptions;
		}

		public static MapBuilder2<byte[], byte[]> fromDictionary(LLDictionary dictionary) {
			return new MapBuilder2<>(dictionary, SerializationOptions.noop());
		}

		public <K2, V2> MapBuilder2<K2, V2> serialize(SerializationOptions<K, K2, V, V2> serializationOptions) {
			return new MapBuilder2<>(this, serializationOptions);
		}

		public static class SerializationOptions<K1, K2, V1, V2> {

			public static SerializationOptions<byte[], byte[], byte[], byte[]> noop() {
				return new SerializationOptions<>();
			}
		}
	}
}
