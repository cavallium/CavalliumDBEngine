package it.cavallium.dbengine.database;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.util.Send;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.StringJoiner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

public class LLDelta extends SimpleResource implements DiscardingCloseable {

	@Nullable
	private final Buffer previous;
	@Nullable
	private final Buffer current;

	private LLDelta(@Nullable Buffer previous, @Nullable Buffer current) {
		super();
		this.previous = previous != null ? previous.makeReadOnly() : null;
		this.current = current != null ? current.makeReadOnly() : null;
	}

	@Override
	protected void ensureOpen() {
		super.ensureOpen();
		assert previous == null || previous.isAccessible();
		assert current == null || current.isAccessible();
	}

	@Override
	protected void onClose() {
		if (previous != null && previous.isAccessible()) {
			previous.close();
		}
		if (current != null && current.isAccessible()) {
			current.close();
		}
	}

	public static LLDelta of(Buffer previous, Buffer current) {
		assert (previous == null && current == null) || (previous != current);
		return new LLDelta(previous, current);
	}

	public Send<Buffer> previous() {
		ensureOpen();
		return previous != null ? previous.copy().send() : null;
	}

	public Send<Buffer> current() {
		ensureOpen();
		return current != null ? current.copy().send() : null;
	}

	public Buffer currentUnsafe() {
		ensureOpen();
		return current;
	}

	public Buffer previousUnsafe() {
		ensureOpen();
		return previous;
	}

	public boolean isModified() {
		return !LLUtils.equals(previous, current);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LLDelta LLDelta = (LLDelta) o;
		return LLUtils.equals(previous, LLDelta.previous) && LLUtils.equals(current, LLDelta.current);
	}

	@Override
	public int hashCode() {
		int result = LLUtils.hashCode(previous);
		result = 31 * result + LLUtils.hashCode(current);
		return result;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLDelta.class.getSimpleName() + "[", "]")
				.add("min=" + LLUtils.toStringSafe(previous))
				.add("max=" + LLUtils.toStringSafe(current))
				.toString();
	}

}
