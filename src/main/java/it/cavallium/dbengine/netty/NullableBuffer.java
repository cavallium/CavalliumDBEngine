package it.cavallium.dbengine.netty;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.SearchResult;
import org.jetbrains.annotations.Nullable;

public class NullableBuffer extends ResourceSupport<NullableBuffer, NullableBuffer> {

	@Nullable
	private Buffer buffer;

	public NullableBuffer(@Nullable Buffer buffer, Drop<NullableBuffer> drop) {
		super(new CloseOnDrop(drop));
		this.buffer = buffer == null ? null : buffer.send().receive();
	}

	public NullableBuffer(@Nullable Send<Buffer> buffer, Drop<NullableBuffer> drop) {
		super(new CloseOnDrop(drop));
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
		return drop -> new NullableBuffer(buffer, drop);
	}

	protected void makeInaccessible() {
		this.buffer = null;
	}

	private static class CloseOnDrop implements Drop<NullableBuffer> {

		private final Drop<NullableBuffer> delegate;

		public CloseOnDrop(Drop<NullableBuffer> drop) {
			this.delegate = drop;
		}

		@Override
		public void drop(NullableBuffer obj) {
			if (obj.buffer != null) {
				if (obj.buffer.isAccessible()) {
					obj.buffer.close();
				}
			}
			delegate.drop(obj);
		}
	}
}
