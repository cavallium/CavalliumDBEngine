package it.cavallium.dbengine.database;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.Drop;
import io.netty.buffer.api.Owned;
import io.netty.buffer.api.Send;
import io.netty.buffer.api.internal.ResourceSupport;
import java.util.StringJoiner;
import org.jetbrains.annotations.NotNull;

public class LLEntry extends ResourceSupport<LLEntry, LLEntry> {
	@NotNull
	private final Buffer key;
	@NotNull
	private final Buffer value;

	private LLEntry(Send<Buffer> key, Send<Buffer> value, Drop<LLEntry> drop) {
		super(new LLEntry.CloseOnDrop(drop));
		this.key = key.receive().makeReadOnly();
		this.value = value.receive().makeReadOnly();
		assert isAllAccessible();
	}

	private boolean isAllAccessible() {
		assert key.isAccessible();
		assert value.isAccessible();
		assert this.isAccessible();
		assert this.isOwned();
		return true;
	}

	public static LLEntry of(Send<Buffer> key, Send<Buffer> value) {
		return new LLEntry(key, value, d -> {});
	}

	public Send<Buffer> getKey() {
		ensureOwned();
		return key.copy().send();
	}

	public Buffer getKeyUnsafe() {
		return key;
	}

	public Send<Buffer> getValue() {
		ensureOwned();
		return value.copy().send();
	}


	public Buffer getValueUnsafe() {
		return value;
	}

	private void ensureOwned() {
		assert isAllAccessible();
		if (!isOwned()) {
			if (!isAccessible()) {
				throw this.createResourceClosedException();
			} else {
				throw new IllegalStateException("Resource not owned");
			}
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LLEntry LLEntry = (LLEntry) o;
		return LLUtils.equals(key, LLEntry.key) && LLUtils.equals(value, LLEntry.value);
	}

	@Override
	public int hashCode() {
		int result = LLUtils.hashCode(key);
		result = 31 * result + LLUtils.hashCode(value);
		return result;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLEntry.class.getSimpleName() + "[", "]")
				.add("key=" + LLUtils.toString(key))
				.add("value=" + LLUtils.toString(value))
				.toString();
	}

	public LLEntry copy() {
		ensureOwned();
		return new LLEntry(key.copy().send(), value.copy().send(), d -> {});
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LLEntry> prepareSend() {
		Send<Buffer> keySend;
		Send<Buffer> valueSend;
		keySend = this.key.send();
		valueSend = this.value.send();
		return drop -> new LLEntry(keySend, valueSend, drop);
	}

	private static class CloseOnDrop implements Drop<LLEntry> {

		private final Drop<LLEntry> delegate;

		public CloseOnDrop(Drop<LLEntry> drop) {
			this.delegate = drop;
		}

		@Override
		public void drop(LLEntry obj) {
			if (obj.key.isAccessible()) {
				obj.key.close();
			}
			if (obj.value.isAccessible()) {
				obj.value.close();
			}
			delegate.drop(obj);
		}
	}
}
