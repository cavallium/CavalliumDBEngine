package it.cavallium.dbengine.database.disk.rocksdb;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.utils.SimpleResource;
import org.rocksdb.AbstractSlice;
import org.rocksdb.ColumnFamilyHandle;

public final class LLColumnFamilyHandle extends SimpleResource {

	private final ColumnFamilyHandle val;

	public LLColumnFamilyHandle(ColumnFamilyHandle val) {
		this.val = val;
	}

	public ColumnFamilyHandle getNative() {
		ensureOpen();
		return val;
	}

	@Override
	protected void onClose() {
		val.close();
	}
}
