package it.cavallium.dbengine.database;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.Drop;
import io.netty.buffer.api.Owned;
import io.netty.buffer.api.Send;
import io.netty.buffer.api.internal.ResourceSupport;
import java.util.StringJoiner;
import org.jetbrains.annotations.Nullable;

public class LLDelta extends ResourceSupport<LLDelta, LLDelta> {
	@Nullable
	private final Buffer previous;
	@Nullable
	private final Buffer current;

	private LLDelta(@Nullable Send<Buffer> previous, @Nullable Send<Buffer> current, Drop<LLDelta> drop) {
		super(new LLDelta.CloseOnDrop(drop));
		assert isAllAccessible();
		this.previous = previous != null ? previous.receive().makeReadOnly() : null;
		this.current = current != null ? current.receive().makeReadOnly() : null;
	}

	private boolean isAllAccessible() {
		assert previous == null || previous.isAccessible();
		assert current == null || current.isAccessible();
		assert this.isAccessible();
		assert this.isOwned();
		return true;
	}

	public static LLDelta of(Send<Buffer> min, Send<Buffer> max) {
		return new LLDelta(min, max, d -> {});
	}

	public Send<Buffer> previous() {
		ensureOwned();
		return previous != null ? previous.copy().send() : null;
	}

	public Send<Buffer> current() {
		ensureOwned();
		return current != null ? current.copy().send() : null;
	}

	public boolean isModified() {
		return !LLUtils.equals(previous, current);
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
				.add("min=" + LLUtils.toString(previous))
				.add("max=" + LLUtils.toString(current))
				.toString();
	}

	public LLDelta copy() {
		ensureOwned();
		return new LLDelta(previous != null ? previous.copy().send() : null,
				current != null ? current.copy().send() : null,
				d -> {}
		);
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LLDelta> prepareSend() {
		Send<Buffer> minSend;
		Send<Buffer> maxSend;
		minSend = this.previous != null ? this.previous.send() : null;
		maxSend = this.current != null ? this.current.send() : null;
		return drop -> new LLDelta(minSend, maxSend, drop);
	}

	private static class CloseOnDrop implements Drop<LLDelta> {

		private final Drop<LLDelta> delegate;

		public CloseOnDrop(Drop<LLDelta> drop) {
			this.delegate = drop;
		}

		@Override
		public void drop(LLDelta obj) {
			if (obj.previous != null) {
				obj.previous.close();
			}
			if (obj.current != null) {
				obj.current.close();
			}
			delegate.drop(obj);
		}
	}
}
