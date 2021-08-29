package it.cavallium.dbengine.database;

public interface SafeCloseable extends AutoCloseable {

	@Override
	void close();
}
