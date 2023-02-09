package it.cavallium.dbengine.database;

import it.cavallium.dbengine.buffers.Buf;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

public class LLEntry {

	private static final Logger logger = LogManager.getLogger(LLEntry.class);
	private final Buf key;
	private final Buf value;

	private LLEntry(@NotNull Buf key, @NotNull Buf value) {
		this.key = key;
		this.value = value;
	}

	public static LLEntry of(@NotNull Buf key, @NotNull Buf value) {
		return new LLEntry(key, value);
	}

	public static LLEntry copyOf(Buf keyView, Buf valueView) {
		return new LLEntry(keyView.copy(), valueView.copy());
	}

	public Buf getKey() {
		return Objects.requireNonNull(key);
	}

	public Buf getValue() {
		return Objects.requireNonNull(value);
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
}
