package it.cavallium.dbengine.database;

import io.netty5.buffer.Buffer;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class OptionalBuf implements DiscardingCloseable {

	private static final OptionalBuf EMPTY = new OptionalBuf(null);
	private final Buffer buffer;

	private OptionalBuf(@Nullable Buffer buffer) {
		this.buffer = buffer;
	}

	public static OptionalBuf ofNullable(@Nullable Buffer buffer) {
		return new OptionalBuf(buffer);
	}

	public static OptionalBuf of(@NotNull Buffer buffer) {
		Objects.requireNonNull(buffer);
		return new OptionalBuf(buffer);
	}

	public static OptionalBuf empty() {
		return EMPTY;
	}

	@Override
	public void close() {
		if (buffer != null && buffer.isAccessible()) {
			buffer.close();
		}
	}

	@Override
	public String toString() {
		if (buffer != null) {
			return buffer.toString();
		} else {
			return "(empty)";
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

		OptionalBuf that = (OptionalBuf) o;

		return Objects.equals(buffer, that.buffer);
	}

	@Override
	public int hashCode() {
		return buffer != null ? buffer.hashCode() : 0;
	}

	public Buffer get() {
		if (buffer == null) {
			throw new NoSuchElementException();
		}
		return buffer;
	}

	public Buffer orElse(Buffer alternative) {
		if (buffer == null) {
			return alternative;
		}
		return buffer;
	}

	public void ifPresent(Consumer<Buffer> consumer) {
		if (buffer != null) {
			consumer.accept(buffer);
		}
	}

	public boolean isPresent() {
		return buffer != null;
	}

	public boolean isEmpty() {
		return buffer == null;
	}

	public <U> Optional<U> map(Function<Buffer, U> mapper) {
		if (buffer != null) {
			return Optional.of(mapper.apply(buffer));
		} else {
			return Optional.empty();
		}
	}
}
