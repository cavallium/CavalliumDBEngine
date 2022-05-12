package it.cavallium.dbengine.database.disk.rocksdb;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.internal.ResourceSupport;
import org.rocksdb.AbstractSlice;
import org.rocksdb.DirectSlice;

public abstract class LLAbstractSlice<T extends AbstractSlice<U>, U> extends ResourceSupport<LLAbstractSlice<T, U>, LLAbstractSlice<T, U>> {

	protected static final Drop<LLAbstractSlice<?, ?>> DROP = new Drop<>() {
		@Override
		public void drop(LLAbstractSlice obj) {
			if (obj.val != null) {
				obj.val.close();
			}
		}

		@Override
		public Drop<LLAbstractSlice<?, ?>> fork() {
			return this;
		}

		@Override
		public void attach(LLAbstractSlice obj) {

		}
	};

	private T val;

	public LLAbstractSlice(T val) {
		//noinspection unchecked
		super((Drop<LLAbstractSlice<T, U>>) (Drop<?>) DROP);
		this.val = val;
	}

	public T getNative() {
		return val;
	}

	@Override
	protected final void makeInaccessible() {
		this.val = null;
	}

	@Override
	protected final Owned<LLAbstractSlice<T, U>> prepareSend() {
		var val = this.val;
		return drop -> {
			var instance = createInstance(val);
			drop.attach(instance);
			return instance;
		};
	}

	protected abstract LLAbstractSlice<T, U> createInstance(T val);
}
