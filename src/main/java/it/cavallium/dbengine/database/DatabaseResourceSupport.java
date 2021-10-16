package it.cavallium.dbengine.database;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.internal.ResourceSupport;

public abstract class DatabaseResourceSupport<I extends Resource<I>, T extends DatabaseResourceSupport<I, T>>
		extends ResourceSupport<I, T> {

	protected DatabaseResourceSupport(Drop<T> drop) {
		super(drop);
	}

}
