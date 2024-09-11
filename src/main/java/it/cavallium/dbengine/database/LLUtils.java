package it.cavallium.dbengine.database;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.disk.rocksdb.LLReadOptions;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.AbstractNativeReference;

@SuppressWarnings("unused")
public class LLUtils {

	private static final Logger logger = LogManager.getLogger(LLUtils.class);
	public static final Marker MARKER_ROCKSDB = MarkerManager.getMarker("ROCKSDB");

	public static final int INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES = 4096;
	public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocateDirect(0).asReadOnlyBuffer();
	private static final byte[] RESPONSE_TRUE = new byte[]{1};
	private static final byte[] RESPONSE_FALSE = new byte[]{0};
	private static final byte[] RESPONSE_TRUE_BUF = new byte[]{1};
	private static final byte[] RESPONSE_FALSE_BUF = new byte[]{0};
	public static final byte[][] LEXICONOGRAPHIC_ITERATION_SEEKS = new byte[256][1];
	public static final boolean MANUAL_READAHEAD = false;

	public static final boolean FORCE_DISABLE_CHECKSUM_VERIFICATION
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.checksum.disable.force", "false"));

	public static final boolean DEBUG_ALL_DROPS
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.drops.log", "false"));
	public static final boolean DEBUG_ALL_DISCARDS
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.discards.log", "false"));

	private static final Lookup PUBLIC_LOOKUP = MethodHandles.publicLookup();

	private static final MethodHandle IS_ACCESSIBLE_METHOD_HANDLE;

	private static final MethodHandle IS_IN_NON_BLOCKING_THREAD_MH;
	private static final Consumer<Object> NULL_CONSUMER = ignored -> {};
	private static final Buf BUF_TRUE = Buf.wrap(new byte[] {(byte) 1});
	private static final Buf BUF_FALSE = Buf.wrap(new byte[] {(byte) 0});
	public static final HexFormat HEX_FORMAT = HexFormat.of().withUpperCase();

	static {
		for (int i1 = 0; i1 < 256; i1++) {
			var b = LEXICONOGRAPHIC_ITERATION_SEEKS[i1];
			b[0] = (byte) i1;
		}
		{
			var methodType = MethodType.methodType(boolean.class);
			MethodHandle isAccessibleMethodHandle = null;
			try {
				isAccessibleMethodHandle = PUBLIC_LOOKUP.findVirtual(AbstractNativeReference.class, "isAccessible", methodType);
			} catch (NoSuchMethodException e) {
				logger.debug("Failed to find isAccessible(): no such method");
			} catch (IllegalAccessException e) {
				logger.debug("Failed to find isAccessible()", e);
			}
			IS_ACCESSIBLE_METHOD_HANDLE = isAccessibleMethodHandle;
		}
		{
			MethodHandle isInNonBlockingThreadMethodHandle = null;
			try {
				var clz = Objects.requireNonNull(PUBLIC_LOOKUP.findClass("reactor.core.scheduler.Schedulers"),
						"reactor.core.scheduler.Schedulers not found");

				var methodType = MethodType.methodType(boolean.class);
				isInNonBlockingThreadMethodHandle = PUBLIC_LOOKUP.findStatic(clz, "isInNonBlockingThread", methodType);
			} catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | NullPointerException e) {
				logger.debug("Failed to obtain access to reactor core schedulers");
			}
			IS_IN_NON_BLOCKING_THREAD_MH = isInNonBlockingThreadMethodHandle;
		}
	}

	public static boolean responseToBoolean(byte[] response) {
		return response[0] == 1;
	}

	public static boolean responseToBoolean(Buf response) {
		if (response == BUF_FALSE) return false;
		if (response == BUF_TRUE) return true;
		assert response.size() == 1;
		return response.getBoolean(0);
	}

	public static byte[] booleanToResponse(boolean bool) {
		return bool ? RESPONSE_TRUE : RESPONSE_FALSE;
	}

	public static Buf booleanToResponseByteBuffer(boolean bool) {
		return bool ? BUF_TRUE : BUF_FALSE;
	}

	private static int[] getIntArray(byte[] data) {
		var count = data.length / Integer.BYTES;
		var items = new int[count];
		for (int i = 0; i < items.length; i++) {
			items[i] = Ints.fromBytes(data[i * Integer.BYTES],
					data[i * Integer.BYTES + 1],
					data[i * Integer.BYTES + 2],
					data[i * Integer.BYTES + 3]
			);
		}
		return items;
	}

	private static long[] getLongArray(byte[] data) {
		var count = data.length / Long.BYTES;
		var items = new long[count];
		for (int i = 0; i < items.length; i++) {
			items[i] = Longs.fromBytes(data[i * Long.BYTES],
					data[i * Long.BYTES + 1],
					data[i * Long.BYTES + 2],
					data[i * Long.BYTES + 3],
					data[i * Long.BYTES + 4],
					data[i * Long.BYTES + 5],
					data[i * Long.BYTES + 6],
					data[i * Long.BYTES + 7]
			);
		}
		return items;
	}

	public static String toStringSafe(byte @Nullable[] key) {
		if (key != null) {
			return toString(key);
		} else {
			return "(released)";
		}
	}

	public static String toStringSafe(@Nullable Buf key) {
		if (key != null) {
			return toString(key);
		} else {
			return "(released)";
		}
	}

	public static String toStringSafe(@Nullable Buf key, int iLimit) {
		if (key != null) {
			return toString(key, iLimit);
		} else {
			return "(released)";
		}
	}

	public static String toStringSafe(@Nullable LLRange range) {
		if (range != null) {
			return toString(range);
		} else {
			return "(released)";
		}
	}

	public static String toString(@Nullable LLRange range) {
		if (range == null) {
			return "null";
		} else if (range.isAll()) {
			return "ξ";
		} else if (range.hasMin() && range.hasMax()) {
			return "[" + toStringSafe(range.getMin()) + "," + toStringSafe(range.getMax()) + ")";
		} else if (range.hasMin()) {
			return "[" + toStringSafe(range.getMin()) + ",*)";
		} else if (range.hasMax()) {
			return "[*," + toStringSafe(range.getMax()) + ")";
		} else {
			return "∅";
		}
	}

	public static String toString(@Nullable Buf key) {
		if (key != null) {
			return toString(key.asArray());
		} else {
			return "null";
		}
	}

	public static String toString(@Nullable Buf key, int iLimit) {
		if (key != null) {
			return toString(key.asArray(), iLimit);
		} else {
			return "null";
		}
	}

	public static String toString(byte @Nullable[] key) {
		return toString(key, 1024);
	}

	public static String toString(byte @Nullable[] key, int iLimit) {
		if (key == null) {
			return "null";
		} else {
			int startIndex = 0;
			int iMax = key.length - 1;
			if (iMax <= -1) {
				return "\"\"";
			} else {
				StringBuilder arraySB = new StringBuilder();
				StringBuilder asciiSB = new StringBuilder();
				boolean isAscii = true;
				arraySB.append('[');
				int i = 0;

				while (true) {
					var byteVal = (int) key[startIndex + i];
					arraySB.append(byteVal);
					if (isAscii) {
						if (byteVal >= 32 && byteVal < 127) {
							asciiSB.append((char) byteVal);
						} else if (byteVal == 0) {
							asciiSB.append('␀');
						} else {
							isAscii = false;
							asciiSB = null;
						}
					}
					if (i == iLimit) {
						arraySB.append("…");
					}
					if (i == iMax || i == iLimit) {
						if (isAscii) {
							return asciiSB.insert(0, "\"").append("\"").toString();
						} else {
							if (i >= iLimit) {
								return arraySB.append(']').toString();
							} else {
								return HEX_FORMAT.formatHex(key);
							}
						}
					}

					arraySB.append(", ");
					++i;
				}
			}
		}
	}

	public static byte[] parseHex(String hex) {
		return HEX_FORMAT.parseHex(hex);
	}

	public static boolean equals(Buf a, Buf b) {
		if (a == null && b == null) {
			return true;
		} else if (a != null && b != null) {
			return a.equals(b);
		} else {
			return false;
		}
	}


	/**
	 * Returns {@code true} if and only if the two specified buffers are identical to each other for {@code length} bytes
	 * starting at {@code aStartIndex} index for the {@code a} buffer and {@code bStartIndex} index for the {@code b}
	 * buffer. A more compact way to express this is:
	 * <p>
	 * {@code a[aStartIndex : aStartIndex + length] == b[bStartIndex : bStartIndex + length]}
	 */
	public static boolean equals(Buf a, int aStartIndex, Buf b, int bStartIndex, int length) {
		return a.equals(aStartIndex, b, bStartIndex, length);
	}

	/**
	 *
	 * @return the inner array, DO NOT MODIFY IT
	 */
	public static byte[] asArray(@Nullable Buf key) {
		if (key == null) {
			return EMPTY_BYTE_ARRAY;
		}
		return key.asArray();
	}

	public static int hashCode(Buf buf) {
		if (buf == null) {
			return 0;
		}

		return buf.hashCode();
	}

	public static boolean isBoundedRange(LLRange rangeShared) {
		return rangeShared.hasMin() && rangeShared.hasMax();
	}

	/**
	 * Generate a ReadOptions, with some parameters modified to help with bulk iterations
	 * @param readOptions the read options to start with, it will be modified
	 * @param canFillCache true to fill the cache. If closedRange is false, this field will be ignored
	 * @param boundedRange true if the range is bounded from both sides
	 * @param smallRange true if the range is small
	 * @return the passed instance of ReadOptions, or a new one if the passed readOptions is null
	 */
	public static LLReadOptions generateCustomReadOptions(@Nullable LLReadOptions readOptions,
			boolean canFillCache,
			boolean boundedRange,
			boolean smallRange) {
		if (readOptions == null) {
			//noinspection resource
			readOptions = new LLReadOptions();
		}
		var hugeRange = !boundedRange && !smallRange;
		if (hugeRange) {
			if (readOptions.readaheadSize() <= 0) {
				readOptions.setReadaheadSize(4 * 1024 * 1024); // 4MiB
			}
		}
		readOptions.setFillCache(canFillCache && !hugeRange);
		readOptions.setVerifyChecksums(!FORCE_DISABLE_CHECKSUM_VERIFICATION || !hugeRange);

		return readOptions;
	}

	public static void finalizeResource(SafeCloseable resource) {
		resource.close();
	}

	public static void finalizeResourceNow(SafeCloseable resource) {
		resource.close();
	}

	public static boolean isAccessible(AbstractNativeReference abstractNativeReference) {
		if (IS_ACCESSIBLE_METHOD_HANDLE != null) {
			try {
				return (boolean) IS_ACCESSIBLE_METHOD_HANDLE.invoke(abstractNativeReference);
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
		}
		return true;
	}

	public static Buf unmodifiableBytes(Buf previous) {
		if (previous == null) {
			return null;
		}
		previous.freeze();
		return previous;
	}

	public static boolean isInNonBlockingThread() {
		if (IS_IN_NON_BLOCKING_THREAD_MH != null) {
			try {
				return (boolean) IS_IN_NON_BLOCKING_THREAD_MH.invokeExact();
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
		}
		return false;
	}

	public static Buf copy(Buf buf) {
		return buf.copy();
	}

	public static Buf asByteList(byte[] array) {
		return Buf.wrap(array);
	}

	public static Buf toByteList(byte[] array) {
		return Buf.copyOf(array);
	}

	public static <T> T resolveDelta(Delta<T> delta, UpdateReturnMode updateReturnMode) {
		return switch (updateReturnMode) {
			case GET_NEW_VALUE -> delta.current();
			case GET_OLD_VALUE -> delta.previous();
			case NOTHING -> null;
		};
	}

	public static Buf resolveLLDelta(LLDelta delta, UpdateReturnMode updateReturnMode) {
		final Buf previous = delta.previous();
		final Buf current = delta.current();
		return switch (updateReturnMode) {
			case GET_NEW_VALUE -> current;
			case GET_OLD_VALUE -> previous;
			case NOTHING -> null;
		};
	}

	public static <T, U> Delta<U> mapDelta(Delta<T> delta, SerializationFunction<@NotNull T, @Nullable U> mapper) {
		T prev = delta.previous();
		T curr = delta.current();
		U newPrev;
		U newCurr;
		if (prev != null) {
			newPrev = mapper.apply(prev);
		} else {
			newPrev = null;
		}
		if (curr != null) {
			newCurr = mapper.apply(curr);
		} else {
			newCurr = null;
		}
		return new Delta<>(newPrev, newCurr);
	}

	public static <U> Delta<U> mapLLDelta(LLDelta delta, SerializationFunction<@NotNull Buf, @Nullable U> mapper) {
		var prev = delta.previous();
		var curr = delta.current();
		U newPrev;
		U newCurr;
		if (prev != null) {
			newPrev = mapper.apply(prev);
		} else {
			newPrev = null;
		}
		if (curr != null) {
			newCurr = mapper.apply(curr);
		} else {
			newCurr = null;
		}
		return new Delta<>(newPrev, newCurr);
	}

	public static <R, V> boolean isDeltaChanged(Delta<V> delta) {
		return !Objects.equals(delta.previous(), delta.current());
	}

	public static int utf8MaxBytes(String deserialized) {
		return deserialized.length() * 3;
	}

	private static void onNextDropped(Object next) {
		if (DEBUG_ALL_DROPS) {
			logger.trace("Dropped: {}", () -> next.getClass().getName());
		}
		closeResource(next, false);
	}

	public static void onDiscard(Object next) {
		if (DEBUG_ALL_DISCARDS) {
			logger.trace("Discarded: {}", () -> next.getClass().getName());
		}
		closeResource(next, false);
	}

	public static void closeResource(Object next) {
		closeResource(next, true);
	}

	private static void closeResource(Object next, boolean manual) {
		if (next instanceof SafeCloseable closeable) {
			if (manual || closeable instanceof DiscardingCloseable) {
				closeable.close();
			}
		} else if (next instanceof List<?> iterable) {
			iterable.forEach(obj -> closeResource(obj, manual));
		} else if (next instanceof Set<?> iterable) {
			iterable.forEach(obj -> closeResource(obj, manual));
		} else if (next instanceof AbstractImmutableNativeReference rocksObj) {
			if (rocksObj.isOwningHandle()) {
				rocksObj.close();
			}
		} else if (next instanceof Optional<?> optional) {
			optional.ifPresent(obj -> closeResource(obj, manual));
		} else if (next instanceof Map.Entry<?, ?> entry) {
			var key = entry.getKey();
			if (key != null) {
				closeResource(key, manual);
			}
			var value = entry.getValue();
			if (value != null) {
				closeResource(value, manual);
			}
		} else if (next instanceof Delta<?> delta) {
			var previous = delta.previous();
			if (previous != null) {
				closeResource(previous, manual);
			}
			var current = delta.current();
			if (current != null) {
				closeResource(current, manual);
			}
		} else if (next instanceof Map<?, ?> map) {
			map.forEach((key, value) -> {
				if (key != null) {
					closeResource(key, manual);
				}
				if (value != null) {
					closeResource(value, manual);
				}
			});
		}
	}

	public static <T, U> List<U> mapList(Collection<T> input, Function<T, U> mapper) {
		var result = new ArrayList<U>(input.size());
		input.forEach(t -> result.add(mapper.apply(t)));
		return result;
	}

	public static Buf wrapNullable(byte[] array) {
		return array != null ? Buf.wrap(array) : null;
	}
}
