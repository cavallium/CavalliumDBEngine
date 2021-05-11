package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.Mapper;

public class CastMapper<T, U> implements Mapper<T, U> {

	@Override
	public U map(T key) {
		return (U) key;
	}

	@Override
	public T unmap(U key) {
		return (T) key;
	}
}
