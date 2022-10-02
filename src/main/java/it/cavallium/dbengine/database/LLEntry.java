package it.cavallium.dbengine.database;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.Drop;
import io.netty5.buffer.Owned;
import io.netty5.util.Resource;
import io.netty5.util.Send;
import io.netty5.buffer.internal.ResourceSupport;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LLEntry extends SimpleResource implements DiscardingCloseable {

	private static final Logger logger = LogManager.getLogger(LLEntry.class);
	private Buffer key;
	private Buffer value;

	private LLEntry(@NotNull Send<Buffer> key, @NotNull Send<Buffer> value) {
		this.key = key.receive();
		this.value = value.receive();
		assert isAllAccessible();
	}
	private LLEntry(@NotNull Buffer key, @NotNull Buffer value) {
		this.key = key;
		this.value = value;
		assert isAllAccessible();
	}

	private boolean isAllAccessible() {
		assert key != null && key.isAccessible();
		assert value != null && value.isAccessible();
		return true;
	}

	public static LLEntry of(@NotNull Buffer key, @NotNull Buffer value) {
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
	protected void onClose() {
		try {
			if (key != null && key.isAccessible()) {
				key.close();
			}
		} catch (Throwable ex) {
			logger.error("Failed to close key", ex);
		}
		try {
			if (value != null && value.isAccessible()) {
				value.close();
			}
		} catch (Throwable ex) {
			logger.error("Failed to close value", ex);
		}
		key = null;
		value = null;
	}
}
