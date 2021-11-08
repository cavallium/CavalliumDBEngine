package it.cavallium.dbengine.database.serialization;

@FunctionalInterface
public interface KVSerializationFunction<K, V, UV> {

	UV apply(K key, V value) throws SerializationException;
}
