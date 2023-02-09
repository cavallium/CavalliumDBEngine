package it.cavallium.dbengine.client;

public class CastMapper<T, U> implements Mapper<T, U> {

	@SuppressWarnings("unchecked")
	@Override
	public U map(T key) {
		return (U) key;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T unmap(U key) {
		return (T) key;
	}
}
