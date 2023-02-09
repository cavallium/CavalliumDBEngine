package it.cavallium.dbengine.database.serialization;

@FunctionalInterface
public interface SerializationFunction<T, U> {

	U apply(T argument) throws SerializationException;
}
