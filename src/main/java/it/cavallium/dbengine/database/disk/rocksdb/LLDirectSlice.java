package it.cavallium.dbengine.database.disk.rocksdb;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.internal.ResourceSupport;
import java.nio.ByteBuffer;
import org.rocksdb.AbstractSlice;
import org.rocksdb.DirectSlice;

public final class LLDirectSlice extends LLAbstractSlice<DirectSlice, ByteBuffer> {

	public LLDirectSlice(DirectSlice val) {
		super(val);
	}

	public DirectSlice getNative() {
		return super.getNative();
	}

	@Override
	protected LLAbstractSlice<DirectSlice, ByteBuffer> createInstance(DirectSlice val) {
		return new LLDirectSlice(val);
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}
}
