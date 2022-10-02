package it.cavallium.dbengine.database.disk.rocksdb;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.Drop;
import io.netty5.buffer.Owned;
import io.netty5.util.Send;
import io.netty5.buffer.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLLocalGroupedReactiveRocksIterator;
import it.cavallium.dbengine.utils.SimpleResource;
import org.rocksdb.ReadOptions;

public final class LLReadOptions extends SimpleResource {

	private final ReadOptions val;

	public LLReadOptions(ReadOptions val) {
		this.val = val;
	}

	@Override
	protected void onClose() {
		val.close();
	}
}
