package it.cavallium.dbengine.database.disk.rocksdb;

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
