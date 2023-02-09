package it.cavallium.dbengine.database.disk.rocksdb;

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
