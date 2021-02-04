package it.cavallium.dbengine.database.serialization;

public interface Codecs<A> {

	void registerCodec(int id, Codec<A> serializer);

	Codec<A> getCodec(int id);
}
