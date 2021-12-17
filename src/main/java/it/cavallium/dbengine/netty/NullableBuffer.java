package it.cavallium.dbengine.netty;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

public class NullableBuffer extends ResourceSupport<NullableBuffer, NullableBuffer> {

	private static final Logger logger = LogManager.getLogger(NullableBuffer.class);

	private static final Drop<NullableBuffer> DROP = new Drop<>() {
		@Override
		public void drop(NullableBuffer obj) {
			try {
				if (obj.buffer != null) {
					obj.buffer.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close buffer", ex);
			}
		}

		@Override
		public Drop<NullableBuffer> fork() {
			return this;
		}

		@Override
		public void attach(NullableBuffer obj) {

		}
	};

	@Nullable
	private Buffer buffer;

	public NullableBuffer(@Nullable Buffer buffer) {
		super(DROP);
		this.buffer = buffer == null ? null : buffer.send().receive();
	}

	public NullableBuffer(@Nullable Send<Buffer> buffer) {
		super(DROP);
		this.buffer = buffer == null ? null : buffer.receive();
	}

	@Nullable
	public Buffer buf() {
		return buffer;
	}

	@Nullable
	public Send<Buffer> sendBuf() {
		return buffer == null ? null : buffer.send();
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<NullableBuffer> prepareSend() {
		var buffer = this.buffer == null ? null : this.buffer.send();
		return drop -> new NullableBuffer(buffer);
	}

	protected void makeInaccessible() {
		this.buffer = null;
	}
}
