package it.cavallium.dbengine.database.disk.rocksdb;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.internal.ResourceSupport;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.AbstractNativeReference;

public class RocksObj<T extends AbstractNativeReference> extends ResourceSupport<RocksObj<T>, RocksObj<T>> {

	private static final Drop<RocksObj<?>> DROP = new Drop<>() {
		@Override
		public void drop(RocksObj obj) {
			if (obj.val != null) {
				if (obj.val.isAccessible()) obj.val.close();
			}
		}

		@Override
		public Drop<RocksObj<?>> fork() {
			return this;
		}

		@Override
		public void attach(RocksObj obj) {

		}
	};

	private T val;

	public RocksObj(T val) {
		//noinspection unchecked
		super((Drop<RocksObj<T>>) (Drop<?>) DROP);
		this.val = val;
	}

	@NotNull
	public T v() {
		return val;
	}

	@Override
	protected void makeInaccessible() {
		this.val = null;
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<RocksObj<T>> prepareSend() {
		var val = this.val;
		return drop -> {
			var instance = new RocksObj<>(val);
			drop.attach(instance);
			return instance;
		};
	}
}
