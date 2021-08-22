package it.cavallium.dbengine.database.serialization;

import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface SerializationFunction<T, U> {

	U apply(T argument) throws SerializationException;
}
