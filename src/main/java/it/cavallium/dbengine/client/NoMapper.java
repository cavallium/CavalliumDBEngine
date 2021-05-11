package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.Mapper;

public class NoMapper<T> implements Mapper<T, T> {

	@Override
	public T map(T key) {
		return key;
	}

	@Override
	public T unmap(T key) {
		return key;
	}
}
