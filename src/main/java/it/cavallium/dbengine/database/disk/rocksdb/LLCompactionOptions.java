package it.cavallium.dbengine.database.disk.rocksdb;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.internal.ResourceSupport;
import org.rocksdb.CompactionOptions;

public final class LLCompactionOptions extends ResourceSupport<LLCompactionOptions, LLCompactionOptions> {

	private static final Drop<LLCompactionOptions> DROP = new Drop<>() {
		@Override
		public void drop(LLCompactionOptions obj) {
			if (obj.val != null) {
				obj.val.close();
			}
		}

		@Override
		public Drop<LLCompactionOptions> fork() {
			return this;
		}

		@Override
		public void attach(LLCompactionOptions obj) {

		}
	};

	private CompactionOptions val;

	public LLCompactionOptions(CompactionOptions val) {
		super(DROP);
		this.val = val;
	}

	public CompactionOptions getNative() {
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
	protected Owned<LLCompactionOptions> prepareSend() {
		var val = this.val;
		return drop -> {
			var instance = new LLCompactionOptions(val);
			drop.attach(instance);
			return instance;
		};
	}
}
