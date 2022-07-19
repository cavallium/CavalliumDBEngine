package it.cavallium.dbengine.database.disk.rocksdb;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.utils.SimpleResource;
import org.rocksdb.CompactionOptions;

public final class LLCompactionOptions extends SimpleResource {

	private final CompactionOptions val;

	public LLCompactionOptions(CompactionOptions val) {
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
