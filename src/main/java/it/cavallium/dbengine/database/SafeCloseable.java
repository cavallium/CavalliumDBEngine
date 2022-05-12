package it.cavallium.dbengine.database;

public interface SafeCloseable extends io.netty5.util.SafeCloseable {

	@Override
	void close();
}
