package it.cavallium.dbengine.database.disk.rocksdb;

import it.cavallium.dbengine.utils.SimpleResource;
import org.rocksdb.CompactionOptions;

public final class LLCompactionOptions extends SimpleResource {

	private final CompactionOptions val;

	public LLCompactionOptions(CompactionOptions val) {
		super(val::close);
		this.val = val;
	}

	public CompactionOptions getNative() {
		ensureOpen();
		return val;
	}

	@Override
	protected void onClose() {
		val.close();
	}
}
