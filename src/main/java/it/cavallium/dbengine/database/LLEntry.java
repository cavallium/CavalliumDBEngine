package it.cavallium.dbengine.database;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Send;
import java.util.Objects;
import java.util.StringJoiner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;

public class LLEntry extends DatabaseResourceSupport<LLEntry, LLEntry> {

	private static final Logger logger = LoggerFactory.getLogger(LLEntry.class);

	private static final Drop<LLEntry> DROP = new Drop<>() {
		@Override
		public void drop(LLEntry obj) {
			try {
				if (obj.key != null) {
					obj.key.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close key", ex);
			}
			try {
				if (obj.value != null) {
					obj.value.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close value", ex);
			}
		}

		@Override
		public Drop<LLEntry> fork() {
			return this;
		}

		@Override
		public void attach(LLEntry obj) {

		}
	};

	@Nullable
	private Buffer key;
	@Nullable
	private Buffer value;

	private LLEntry(@NotNull Send<Buffer> key, @NotNull Send<Buffer> value) {
		super(DROP);
		this.key = key.receive().makeReadOnly();
		this.value = value.receive().makeReadOnly();
		assert isAllAccessible();
	}

	private boolean isAllAccessible() {
		assert key != null && key.isAccessible();
		assert value != null && value.isAccessible();
		assert this.isAccessible();
		assert this.isOwned();
		return true;
	}

	public static LLEntry of(@NotNull Send<Buffer> key, @NotNull Send<Buffer> value) {
		return new LLEntry(key, value);
	}

	public Send<Buffer> getKey() {
		ensureOwned();
		return Objects.requireNonNull(key).copy().send();
	}

	public Buffer getKeyUnsafe() {
		return key;
	}

	public Send<Buffer> getValue() {
		ensureOwned();
		return Objects.requireNonNull(value).copy().send();
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
	protected void makeInaccessible() {
		this.key = null;
		this.value = null;
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

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LLEntry> prepareSend() {
		Send<Buffer> keySend;
		Send<Buffer> valueSend;
		keySend = Objects.requireNonNull(this.key).send();
		valueSend = Objects.requireNonNull(this.value).send();
		return drop -> {
			var instance = new LLEntry(keySend, valueSend);
			drop.attach(instance);
			return instance;
		};
	}
}
