package it.cavallium.dbengine.database.disk.rocksdb;

import io.netty5.buffer.Drop;
import io.netty5.buffer.Owned;
import io.netty5.buffer.internal.ResourceSupport;
import it.cavallium.dbengine.utils.SimpleResource;
import org.rocksdb.WriteOptions;

public final class LLWriteOptions extends SimpleResource {

	private final WriteOptions val;

	public LLWriteOptions(WriteOptions val) {
		this.val = val;
	}

	@Override
	protected void onClose() {
		val.close();
	}
}
