package it.cavallium.dbengine.database;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.CompositeBuffer;
import io.netty.buffer.api.Send;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.internal.PlatformDependent;
import it.cavallium.dbengine.database.disk.MemorySegmentUtils;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.lucene.RandomSortField;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.ToIntFunction;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;

@SuppressWarnings("unused")
public class LLUtils {

	private static final Logger logger = LoggerFactory.getLogger(LLUtils.class);

	private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocateDirect(0);
	private static final byte[] RESPONSE_TRUE = new byte[]{1};
	private static final byte[] RESPONSE_FALSE = new byte[]{0};
	private static final byte[] RESPONSE_TRUE_BUF = new byte[]{1};
	private static final byte[] RESPONSE_FALSE_BUF = new byte[]{0};
	public static final byte[][] LEXICONOGRAPHIC_ITERATION_SEEKS = new byte[256][1];

	static {
		for (int i1 = 0; i1 < 256; i1++) {
			var b = LEXICONOGRAPHIC_ITERATION_SEEKS[i1];
			b[0] = (byte) i1;
		}
	}

	public static boolean responseToBoolean(byte[] response) {
		return response[0] == 1;
	}

	public static boolean responseToBoolean(Send<Buffer> responseToReceive) {
		try (var response = responseToReceive.receive()) {
			assert response.readableBytes() == 1;
			return response.getByte(response.readerOffset()) == 1;
		}
	}

	public static byte[] booleanToResponse(boolean bool) {
		return bool ? RESPONSE_TRUE : RESPONSE_FALSE;
	}

	public static Send<Buffer> booleanToResponseByteBuffer(BufferAllocator alloc, boolean bool) {
		return alloc.allocate(1).writeByte(bool ? (byte) 1 : 0).send();
	}

	@Nullable
	public static Sort toSort(@Nullable LLSort sort) {
		if (sort == null) {
			return null;
		}
		if (sort.getType() == LLSortType.LONG) {
			return new Sort(new SortedNumericSortField(sort.getFieldName(), SortField.Type.LONG, sort.isReverse()));
		} else if (sort.getType() == LLSortType.RANDOM) {
			return new Sort(new RandomSortField());
		} else if (sort.getType() == LLSortType.SCORE) {
			return new Sort(SortField.FIELD_SCORE);
		} else if (sort.getType() == LLSortType.DOC) {
			return new Sort(SortField.FIELD_DOC);
		}
		return null;
	}

	public static ScoreMode toScoreMode(LLScoreMode scoreMode) {
		return switch (scoreMode) {
			case COMPLETE -> ScoreMode.COMPLETE;
			case TOP_SCORES -> ScoreMode.TOP_SCORES;
			case COMPLETE_NO_SCORES -> ScoreMode.COMPLETE_NO_SCORES;
			default -> throw new IllegalStateException("Unexpected value: " + scoreMode);
		};
	}

	public static Term toTerm(LLTerm term) {
		return new Term(term.getKey(), term.getValue());
	}

	public static Document toDocument(LLDocument document) {
		Document d = new Document();
		for (LLItem item : document.getItems()) {
			d.add(LLUtils.toField(item));
		}
		return d;
	}

	public static Collection<Document> toDocuments(Collection<LLDocument> document) {
		List<Document> d = new ArrayList<>(document.size());
		for (LLDocument doc : document) {
			d.add(LLUtils.toDocument(doc));
		}
		return d;
	}

	public static Collection<Document> toDocumentsFromEntries(Collection<Entry<LLTerm, LLDocument>> documentsList) {
		ArrayList<Document> results = new ArrayList<>(documentsList.size());
		for (Entry<LLTerm, LLDocument> entry : documentsList) {
			results.add(LLUtils.toDocument(entry.getValue()));
		}
		return results;
	}

	public static Iterable<Term> toTerms(Iterable<LLTerm> terms) {
		List<Term> d = new ArrayList<>();
		for (LLTerm term : terms) {
			d.add(LLUtils.toTerm(term));
		}
		return d;
	}

	private static IndexableField toField(LLItem item) {
		return switch (item.getType()) {
			case IntPoint -> new IntPoint(item.getName(), Ints.fromByteArray(item.getData()));
			case LongPoint -> new LongPoint(item.getName(), Longs.fromByteArray(item.getData()));
			case FloatPoint -> new FloatPoint(item.getName(), ByteBuffer.wrap(item.getData()).getFloat());
			case TextField -> new TextField(item.getName(), item.stringValue(), Field.Store.NO);
			case TextFieldStored -> new TextField(item.getName(), item.stringValue(), Field.Store.YES);
			case SortedNumericDocValuesField -> new SortedNumericDocValuesField(item.getName(),
					Longs.fromByteArray(item.getData())
			);
			case StringField -> new StringField(item.getName(), item.stringValue(), Field.Store.NO);
			case StringFieldStored -> new StringField(item.getName(), item.stringValue(), Field.Store.YES);
		};
	}

	public static it.cavallium.dbengine.database.LLKeyScore toKeyScore(LLKeyScore hit) {
		return new it.cavallium.dbengine.database.LLKeyScore(hit.docId(), hit.score(), hit.key());
	}

	public static String toStringSafe(Buffer key) {
		try {
			if (key.isAccessible()) {
				return toString(key);
			} else {
				return "(released)";
			}
		} catch (IllegalReferenceCountException ex) {
			return "(released)";
		}
	}

	public static String toString(Buffer key) {
		if (key == null) {
			return "null";
		} else {
			int startIndex = key.readerOffset();
			int iMax = key.readableBytes() - 1;
			int iLimit = 128;
			if (iMax <= -1) {
				return "[]";
			} else {
				StringBuilder b = new StringBuilder();
				b.append('[');
				int i = 0;

				while (true) {
					b.append(key.getByte(startIndex + i));
					if (i == iLimit) {
						b.append("…");
					}
					if (i == iMax || i == iLimit) {
						return b.append(']').toString();
					}

					b.append(", ");
					++i;
				}
			}
		}
	}

	public static boolean equals(Buffer a, Buffer b) {
		if (a == null && b == null) {
			return true;
		} else if (a != null && b != null) {
			var aCur = a.openCursor();
			var bCur = b.openCursor();
			if (aCur.bytesLeft() != bCur.bytesLeft()) {
				return false;
			}
			while (aCur.readByte() && bCur.readByte()) {
				if (aCur.getByte() != bCur.getByte()) {
					return false;
				}
			}
			return true;
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
	public static boolean equals(Buffer a, int aStartIndex, Buffer b, int bStartIndex, int length) {
		var aCur = a.openCursor(aStartIndex, length);
		var bCur = b.openCursor(bStartIndex, length);
		if (aCur.bytesLeft() != bCur.bytesLeft()) {
			return false;
		}
		while (aCur.readByte() && bCur.readByte()) {
			if (aCur.getByte() != bCur.getByte()) {
				return false;
			}
		}
		return true;
	}

	public static byte[] toArray(Buffer key) {
		byte[] array = new byte[key.readableBytes()];
		key.copyInto(key.readerOffset(), array, 0, key.readableBytes());
		return array;
	}

	public static List<byte[]> toArray(List<Buffer> input) {
		List<byte[]> result = new ArrayList<>(input.size());
		for (Buffer byteBuf : input) {
			result.add(toArray(byteBuf));
		}
		return result;
	}

	public static int hashCode(Buffer buf) {
		if (buf == null) {
			return 0;
		}

		int result = 1;
		var cur = buf.openCursor();
		while (cur.readByte()) {
			var element = cur.getByte();
			result = 31 * result + element;
		}

		return result;
	}

	/**
	 * @return null if size is equal to RocksDB.NOT_FOUND
	 */
	@SuppressWarnings("ConstantConditions")
	@Nullable
	public static Send<Buffer> readNullableDirectNioBuffer(BufferAllocator alloc, ToIntFunction<ByteBuffer> reader) {
		ByteBuffer directBuffer;
		Buffer buffer;
		{
			var direct = LLUtils.newDirect(alloc, 4096);
			directBuffer = direct.byteBuffer();
			buffer = direct.buffer().receive();
		}
		try {
			int size;
			do {
				directBuffer.limit(directBuffer.capacity());
				assert directBuffer.isDirect();
				size = reader.applyAsInt(directBuffer);
				if (size != RocksDB.NOT_FOUND) {
					if (size == directBuffer.limit()) {
						buffer.readerOffset(0).writerOffset(size);
						return buffer.send();
					} else {
						assert size > directBuffer.limit();
						assert directBuffer.limit() > 0;
						// Free the buffer
						if (directBuffer != null) {
							// todo: check if free is needed
							PlatformDependent.freeDirectBuffer(directBuffer);
							directBuffer = null;
						}
						directBuffer = LLUtils.obtainDirect(buffer);
						buffer.ensureWritable(size);
					}
				}
			} while (size != RocksDB.NOT_FOUND);

			// Return null if size is equal to RocksDB.NOT_FOUND
			return null;
		} finally {
			// Free the buffer
			if (directBuffer != null) {
				// todo: check if free is needed
				PlatformDependent.freeDirectBuffer(directBuffer);
				directBuffer = null;
			}
			buffer.close();
		}
	}

	public static record DirectBuffer(@NotNull Send<Buffer> buffer, @NotNull ByteBuffer byteBuffer) {}

	@NotNull
	public static DirectBuffer newDirect(BufferAllocator allocator, int size) {
		try (var buf = allocator.allocate(size)) {
			var direct = obtainDirect(buf);
			return new DirectBuffer(buf.send(), direct);
		}
	}

	@NotNull
	public static DirectBuffer convertToDirect(BufferAllocator allocator, Send<Buffer> content) {
		try (var buf = content.receive()) {
			if (buf.nativeAddress() != 0) {
				var direct = obtainDirect(buf);
				return new DirectBuffer(buf.send(), direct);
			} else {
				var direct = newDirect(allocator, buf.readableBytes());
				try (var buf2 = direct.buffer().receive()) {
					buf.copyInto(buf.readerOffset(), buf2, buf2.writerOffset(), buf.readableBytes());
					return new DirectBuffer(buf2.send(), direct.byteBuffer());
				}
			}
		}
	}

	@NotNull
	public static ByteBuffer obtainDirect(Buffer buffer) {
		if (!PlatformDependent.hasUnsafe()) {
			throw new UnsupportedOperationException("Please enable unsafe support or disable netty direct buffers",
					PlatformDependent.getUnsafeUnavailabilityCause()
			);
		}
		if (!MemorySegmentUtils.isSupported()) {
			throw new UnsupportedOperationException("Foreign Memory Access API support is disabled."
					+ " Please set \"--enable-preview --add-modules jdk.incubator.foreign -Dforeign.restricted=permit\"");
		}
		assert buffer.isAccessible();
		long nativeAddress;
		if ((nativeAddress = buffer.nativeAddress()) == 0) {
			if (buffer.capacity() == 0) {
				return EMPTY_BYTE_BUFFER;
			}
			throw new IllegalStateException("Buffer is not direct");
		}
		return MemorySegmentUtils.directBuffer(nativeAddress, buffer.capacity());
	}

	public static Buffer fromByteArray(BufferAllocator alloc, byte[] array) {
		Buffer result = alloc.allocate(array.length);
		result.writeBytes(array);
		return result;
	}

	@NotNull
	public static Send<Buffer> readDirectNioBuffer(BufferAllocator alloc, ToIntFunction<ByteBuffer> reader) {
		var nullableSend = readNullableDirectNioBuffer(alloc, reader);
		try (var buffer = nullableSend != null ? nullableSend.receive() : null) {
			if (buffer == null) {
				throw new IllegalStateException("A non-nullable buffer read operation tried to return a \"not found\" element");
			}
			return buffer.send();
		}
	}

	public static Send<Buffer> compositeBuffer(BufferAllocator alloc, Send<Buffer> buffer) {
		try (var composite = buffer.receive()) {
			return composite.send();
		}
	}

	public static Send<Buffer> compositeBuffer(BufferAllocator alloc, Send<Buffer> buffer1, Send<Buffer> buffer2) {
		try (buffer1) {
			try (buffer2) {
				try (var composite = CompositeBuffer.compose(alloc, buffer1, buffer2)) {
					return composite.send();
				}
			}
		}
	}

	public static Send<Buffer> compositeBuffer(BufferAllocator alloc,
			Send<Buffer> buffer1,
			Send<Buffer> buffer2,
			Send<Buffer> buffer3) {
		try (buffer1) {
			try (buffer2) {
				try (buffer3) {
					try (var composite = CompositeBuffer.compose(alloc, buffer1, buffer2, buffer3)) {
						return composite.send();
					}
				}
			}
		}
	}

	@SafeVarargs
	public static Send<Buffer> compositeBuffer(BufferAllocator alloc, Send<Buffer>... buffers) {
		try {
			return switch (buffers.length) {
				case 0 -> alloc.allocate(0).send();
				case 1 -> compositeBuffer(alloc, buffers[0]);
				case 2 -> compositeBuffer(alloc, buffers[0], buffers[1]);
				case 3 -> compositeBuffer(alloc, buffers[0], buffers[1], buffers[2]);
				default -> {
					try (var composite = CompositeBuffer.compose(alloc, buffers)) {
						yield composite.send();
					}
				}
			};
		} finally {
			for (Send<Buffer> buffer : buffers) {
				buffer.close();
			}
		}
	}

	public static <T> Mono<T> resolveDelta(Mono<Delta<T>> prev, UpdateReturnMode updateReturnMode) {
		return prev.handle((delta, sink) -> {
			switch (updateReturnMode) {
				case GET_NEW_VALUE -> {
					var current = delta.current();
					if (current != null) {
						sink.next(current);
					} else {
						sink.complete();
					}
				}
				case GET_OLD_VALUE -> {
					var previous = delta.previous();
					if (previous != null) {
						sink.next(previous);
					} else {
						sink.complete();
					}
				}
				case NOTHING -> sink.complete();
				default -> sink.error(new IllegalStateException());
			}
		});
	}

	public static Mono<Send<Buffer>> resolveLLDelta(Mono<LLDelta> prev, UpdateReturnMode updateReturnMode) {
		return prev.handle((delta, sink) -> {
			try (delta) {
				switch (updateReturnMode) {
					case GET_NEW_VALUE -> {
						var current = delta.current();
						if (current != null) {
							sink.next(current);
						} else {
							sink.complete();
						}
					}
					case GET_OLD_VALUE -> {
						var previous = delta.previous();
						if (previous != null) {
							sink.next(previous);
						} else {
							sink.complete();
						}
					}
					case NOTHING -> sink.complete();
					default -> sink.error(new IllegalStateException());
				}
			}
		});
	}

	public static <T, U> Mono<Delta<U>> mapDelta(Mono<Delta<T>> mono,
			SerializationFunction<@NotNull T, @Nullable U> mapper) {
		return mono.handle((delta, sink) -> {
			try {
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
				sink.next(new Delta<>(newPrev, newCurr));
			} catch (SerializationException ex) {
				sink.error(ex);
			}
		});
	}

	public static <U> Mono<Delta<U>> mapLLDelta(Mono<LLDelta> mono,
			SerializationFunction<@NotNull Send<Buffer>, @Nullable U> mapper) {
		return mono.handle((delta, sink) -> {
			try {
				try (Send<Buffer> prev = delta.previous()) {
					try (Send<Buffer> curr = delta.current()) {
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
						sink.next(new Delta<>(newPrev, newCurr));
					}
				}
			} catch (SerializationException ex) {
				sink.error(ex);
			}
		});
	}

	public static <R, V> boolean isDeltaChanged(Delta<V> delta) {
		return !Objects.equals(delta.previous(), delta.current());
	}

	public static Mono<Send<Buffer>> lazyRetain(Buffer buf) {
		return Mono.just(buf).map(b -> b.copy().send());
	}

	public static Mono<Send<LLRange>> lazyRetainRange(LLRange range) {
		return Mono.just(range).map(r -> r.copy().send());
	}

	public static Mono<Send<Buffer>> lazyRetain(Callable<Send<Buffer>> bufCallable) {
		return Mono.fromCallable(bufCallable);
	}

	public static Mono<Send<LLRange>> lazyRetainRange(Callable<Send<LLRange>> rangeCallable) {
		return Mono.fromCallable(rangeCallable);
	}

	public static <T> Mono<T> handleDiscard(Mono<T> mono) {
		return mono.doOnDiscard(Object.class, obj -> {
			if (obj instanceof SafeCloseable o) {
				discardRefCounted(o);
			} else if (obj instanceof Entry o) {
				discardEntry(o);
			} else if (obj instanceof Collection o) {
				discardCollection(o);
			} else if (obj instanceof Tuple3 o) {
				discardTuple3(o);
			} else if (obj instanceof Tuple2 o) {
				discardTuple2(o);
			} else if (obj instanceof LLEntry o) {
				discardLLEntry(o);
			} else if (obj instanceof LLRange o) {
				discardLLRange(o);
			} else if (obj instanceof Delta o) {
				discardDelta(o);
			} else if (obj instanceof Send o) {
				discardSend(o);
			} else if (obj instanceof Map o) {
				discardMap(o);
			}
		});
		// todo: check if the single object discard hook is more performant
		/*
				.doOnDiscard(SafeCloseable.class, LLUtils::discardRefCounted)
				.doOnDiscard(Map.Entry.class, LLUtils::discardEntry)
				.doOnDiscard(Collection.class, LLUtils::discardCollection)
				.doOnDiscard(Tuple2.class, LLUtils::discardTuple2)
				.doOnDiscard(Tuple3.class, LLUtils::discardTuple3)
				.doOnDiscard(LLEntry.class, LLUtils::discardLLEntry)
				.doOnDiscard(LLRange.class, LLUtils::discardLLRange)
				.doOnDiscard(Delta.class, LLUtils::discardDelta)
				.doOnDiscard(Send.class, LLUtils::discardSend)
				.doOnDiscard(Map.class, LLUtils::discardMap);

		 */
	}

	public static <T> Flux<T> handleDiscard(Flux<T> mono) {
		return mono.doOnDiscard(Object.class, obj -> {
			if (obj instanceof SafeCloseable o) {
				discardRefCounted(o);
			} else if (obj instanceof Entry o) {
				discardEntry(o);
			} else if (obj instanceof Collection o) {
				discardCollection(o);
			} else if (obj instanceof Tuple3 o) {
				discardTuple3(o);
			} else if (obj instanceof Tuple2 o) {
				discardTuple2(o);
			} else if (obj instanceof LLEntry o) {
				discardLLEntry(o);
			} else if (obj instanceof LLRange o) {
				discardLLRange(o);
			} else if (obj instanceof Delta o) {
				discardDelta(o);
			} else if (obj instanceof Send o) {
				discardSend(o);
			} else if (obj instanceof Map o) {
				discardMap(o);
			}
		});
		// todo: check if the single object discard hook is more performant
		/*
				.doOnDiscard(SafeCloseable.class, LLUtils::discardRefCounted)
				.doOnDiscard(Map.Entry.class, LLUtils::discardEntry)
				.doOnDiscard(Collection.class, LLUtils::discardCollection)
				.doOnDiscard(Tuple2.class, LLUtils::discardTuple2)
				.doOnDiscard(Tuple3.class, LLUtils::discardTuple3)
				.doOnDiscard(LLEntry.class, LLUtils::discardLLEntry)
				.doOnDiscard(LLRange.class, LLUtils::discardLLRange)
				.doOnDiscard(Delta.class, LLUtils::discardDelta)
				.doOnDiscard(Send.class, LLUtils::discardSend)
				.doOnDiscard(Map.class, LLUtils::discardMap);

		 */
	}

	private static void discardLLEntry(LLEntry entry) {
		logger.trace("Releasing discarded Buffer");
		entry.close();
	}

	private static void discardLLRange(LLRange range) {
		logger.trace("Releasing discarded Buffer");
		range.close();
	}

	private static void discardEntry(Map.Entry<?, ?> e) {
		if (e.getKey() instanceof Buffer bb) {
			bb.close();
		}
		if (e.getValue() instanceof Buffer bb) {
			bb.close();
		}
	}

	private static void discardTuple2(Tuple2<?, ?> e) {
		if (e.getT1() instanceof Buffer bb) {
			bb.close();
		}
		if (e.getT2() instanceof Buffer bb) {
			bb.close();
		}
	}

	private static void discardTuple3(Tuple3<?, ?, ?> e) {
		if (e.getT1() instanceof Buffer bb) {
			bb.close();
		} else if (e.getT1() instanceof Optional opt) {
			if (opt.isPresent() && opt.get() instanceof Buffer bb) {
				bb.close();
			}
		}
		if (e.getT2() instanceof Buffer bb) {
			bb.close();
		} else if (e.getT1() instanceof Optional opt) {
			if (opt.isPresent() && opt.get() instanceof Buffer bb) {
				bb.close();
			}
		}
		if (e.getT3() instanceof Buffer bb) {
			bb.close();
		} else if (e.getT1() instanceof Optional opt) {
			if (opt.isPresent() && opt.get() instanceof Buffer bb) {
				bb.close();
			}
		}
	}

	private static void discardRefCounted(SafeCloseable safeCloseable) {
		safeCloseable.close();
	}

	private static void discardCollection(Collection<?> collection) {
		for (Object o : collection) {
			if (o instanceof SafeCloseable safeCloseable) {
				safeCloseable.close();
			} else if (o instanceof Map.Entry entry) {
				if (entry.getKey() instanceof SafeCloseable bb) {
					bb.close();
				}
				if (entry.getValue() instanceof SafeCloseable bb) {
					bb.close();
				}
			} else {
				break;
			}
		}
	}

	private static void discardDelta(Delta<?> delta) {
		if (delta.previous() instanceof Buffer bb) {
			bb.close();
		}
		if (delta.current() instanceof Buffer bb) {
			bb.close();
		}
	}

	private static void discardSend(Send<?> send) {
		send.close();
	}

	private static void discardMap(Map<?, ?> map) {
		for (Entry<?, ?> entry : map.entrySet()) {
			boolean hasByteBuf = false;
			if (entry.getKey() instanceof Buffer bb) {
				bb.close();
				hasByteBuf = true;
			}
			if (entry.getValue() instanceof Buffer bb) {
				bb.close();
				hasByteBuf = true;
			}
			if (!hasByteBuf) {
				break;
			}
		}
	}

	public static boolean isDirect(Buffer key) {
		var readableComponents = key.countReadableComponents();
		if (readableComponents == 0) {
			return true;
		} else if (readableComponents == 1) {
			return key.forEachReadable(0, (index, component) -> component.readableBuffer().isDirect()) >= 0;
		} else {
			return false;
		}
	}

	public static String deserializeString(Send<Buffer> bufferSend, int readerOffset, int length, Charset charset) {
		try (var buffer = bufferSend.receive()) {
			byte[] bytes = new byte[Math.min(length, buffer.readableBytes())];
			buffer.copyInto(readerOffset, bytes, 0, length);
			return new String(bytes, charset);
		}
	}

	public static int utf8MaxBytes(String deserialized) {
		return deserialized.length() * 3;
	}
}
