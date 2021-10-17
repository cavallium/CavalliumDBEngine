package it.cavallium.dbengine.database;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import java.util.StringJoiner;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;

public class LLDelta extends ResourceSupport<LLDelta, LLDelta> {

	private static final Logger logger = LoggerFactory.getLogger(LLDelta.class);

	private static final Drop<LLDelta> DROP = new Drop<>() {
		@Override
		public void drop(LLDelta obj) {
			try {
				if (obj.previous != null) {
					obj.previous.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close previous", ex);
			}
			try {
				if (obj.current != null) {
					obj.current.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close current", ex);
			}
			try {
				if (obj.onClose != null) {
					obj.onClose.run();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close onDrop", ex);
			}
		}

		@Override
		public Drop<LLDelta> fork() {
			return this;
		}

		@Override
		public void attach(LLDelta obj) {

		}
	};

	@Nullable
	private Buffer previous;
	@Nullable
	private Buffer current;
	@Nullable
	private Runnable onClose;

	private LLDelta(@Nullable Send<Buffer> previous, @Nullable Send<Buffer> current, @Nullable Runnable onClose) {
		super(DROP);
		assert isAllAccessible();
		this.previous = previous != null ? previous.receive().makeReadOnly() : null;
		this.current = current != null ? current.receive().makeReadOnly() : null;
		this.onClose = onClose;
	}

	private boolean isAllAccessible() {
		assert previous == null || previous.isAccessible();
		assert current == null || current.isAccessible();
		assert this.isAccessible();
		assert this.isOwned();
		return true;
	}

	public static LLDelta of(Send<Buffer> previous, Send<Buffer> current) {
		assert (previous == null && current == null) || (previous != current);
		return new LLDelta(previous, current, null);
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
				.add("min=" + LLUtils.toStringSafe(previous))
				.add("max=" + LLUtils.toStringSafe(current))
				.toString();
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected void makeInaccessible() {
		this.current = null;
		this.previous = null;
		this.onClose = null;
	}

	@Override
	protected Owned<LLDelta> prepareSend() {
		Send<Buffer> minSend = this.previous != null ? this.previous.send() : null;
		Send<Buffer> maxSend = this.current != null ? this.current.send() : null;
		Runnable onClose = this.onClose;
		return drop -> {
			var instance = new LLDelta(minSend, maxSend, onClose);
			drop.attach(instance);
			return instance;
		};
	}

}
