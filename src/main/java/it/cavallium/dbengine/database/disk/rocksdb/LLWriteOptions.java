package it.cavallium.dbengine.database.disk.rocksdb;

import it.cavallium.dbengine.utils.SimpleResource;
import org.rocksdb.WriteOptions;

public final class LLWriteOptions extends SimpleResource {

	private final WriteOptions val;

	public LLWriteOptions(WriteOptions val) {
		super(val::close);
		this.val = val;
	}

	public LLWriteOptions() {
		this(new WriteOptions());
	}

	@Override
	protected void onClose() {
		val.close();
	}

	public WriteOptions getUnsafe() {
		return val;
	}

	public LLWriteOptions setDisableWAL(boolean disableWAL) {
		val.setDisableWAL(disableWAL);
		return this;
	}

	public LLWriteOptions setNoSlowdown(boolean noSlowdown) {
		val.setNoSlowdown(noSlowdown);
		return this;
	}

	public LLWriteOptions setLowPri(boolean lowPri) {
		val.setLowPri(lowPri);
		return this;
	}

	public LLWriteOptions setMemtableInsertHintPerBatch(boolean memtableInsertHintPerBatch) {
		val.setMemtableInsertHintPerBatch(memtableInsertHintPerBatch);
		return this;
	}
}
