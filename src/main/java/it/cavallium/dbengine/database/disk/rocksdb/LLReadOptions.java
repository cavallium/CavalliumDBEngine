package it.cavallium.dbengine.database.disk.rocksdb;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.util.Send;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLLocalGroupedReactiveRocksIterator;
import org.rocksdb.ReadOptions;

public final class LLReadOptions extends ResourceSupport<LLReadOptions, LLReadOptions> {

	private static final Drop<LLReadOptions> DROP = new Drop<>() {
		@Override
		public void drop(LLReadOptions obj) {
			if (obj.val != null) {
				obj.val.close();
			}
		}

		@Override
		public Drop<LLReadOptions> fork() {
			return this;
		}

		@Override
		public void attach(LLReadOptions obj) {

		}
	};

	private ReadOptions val;

	public LLReadOptions(ReadOptions val) {
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
	protected Owned<LLReadOptions> prepareSend() {
		var val = this.val;
		return drop -> {
			var instance = new LLReadOptions(val);
			drop.attach(instance);
			return instance;
		};
	}
}
