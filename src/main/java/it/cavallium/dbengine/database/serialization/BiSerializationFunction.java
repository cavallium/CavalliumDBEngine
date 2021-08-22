package it.cavallium.dbengine.database.serialization;

@FunctionalInterface
public interface BiSerializationFunction<T1, T2, U> {

	U apply(T1 argument1, T2 argument2) throws SerializationException;
}
