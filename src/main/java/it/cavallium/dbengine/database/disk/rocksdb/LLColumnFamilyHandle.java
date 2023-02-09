package it.cavallium.dbengine.database.disk.rocksdb;

import it.cavallium.dbengine.utils.SimpleResource;
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
