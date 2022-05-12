package it.cavallium.dbengine.database.disk.rocksdb;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.internal.ResourceSupport;
import org.rocksdb.AbstractSlice;
import org.rocksdb.ColumnFamilyHandle;

public final class LLColumnFamilyHandle extends ResourceSupport<LLColumnFamilyHandle, LLColumnFamilyHandle> {

	private static final Drop<LLColumnFamilyHandle> DROP = new Drop<>() {
		@Override
		public void drop(LLColumnFamilyHandle obj) {
			if (obj.val != null) {
				obj.val.close();
			}
		}

		@Override
		public Drop<LLColumnFamilyHandle> fork() {
			return this;
		}

		@Override
		public void attach(LLColumnFamilyHandle obj) {

		}
	};

	private ColumnFamilyHandle val;

	public LLColumnFamilyHandle(ColumnFamilyHandle val) {
		super(DROP);
		this.val = val;
	}

	public ColumnFamilyHandle getNative() {
		return val;
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected void makeInaccessible() {
		this.val = null;
	}

	@Override
	protected Owned<LLColumnFamilyHandle> prepareSend() {
		var val = this.val;
		return drop -> {
			var instance = new LLColumnFamilyHandle(val);
			drop.attach(instance);
			return instance;
		};
	}
}
