package it.cavallium.dbengine.client;

public interface Mapper<T, V> {

	V map(T key);

	T unmap(V key);
}
