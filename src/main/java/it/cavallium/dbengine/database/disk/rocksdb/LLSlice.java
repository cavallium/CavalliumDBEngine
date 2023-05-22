package it.cavallium.dbengine.database.disk.rocksdb;

import it.cavallium.dbengine.utils.SimpleResource;
import org.rocksdb.AbstractSlice;
import org.rocksdb.Slice;

public final class LLSlice extends SimpleResource {

	private final AbstractSlice<?> val;

	public LLSlice(AbstractSlice<?> val) {
		super(val::close);
		this.val = val;
	}

	public static LLSlice copyOf(byte[] data) {
		return new LLSlice(new Slice(data));
	}

	public AbstractSlice<?> getSliceUnsafe() {
		return val;
	}

	@Override
	protected void onClose() {
		val.close();
	}
}
