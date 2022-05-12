package it.cavallium.dbengine.database.disk.rocksdb;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.internal.ResourceSupport;
import org.rocksdb.WriteOptions;

public final class LLWriteOptions extends ResourceSupport<LLWriteOptions, LLWriteOptions> {

	private static final Drop<LLWriteOptions> DROP = new Drop<>() {
		@Override
		public void drop(LLWriteOptions obj) {
			if (obj.val != null) {
				obj.val.close();
			}
		}

		@Override
		public Drop<LLWriteOptions> fork() {
			return this;
		}

		@Override
		public void attach(LLWriteOptions obj) {

		}
	};

	private WriteOptions val;

	public LLWriteOptions(WriteOptions val) {
		super(DROP);
		this.val = val;
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
	protected Owned<LLWriteOptions> prepareSend() {
		var val = this.val;
		return drop -> {
			var instance = new LLWriteOptions(val);
			drop.attach(instance);
			return instance;
		};
	}
}
