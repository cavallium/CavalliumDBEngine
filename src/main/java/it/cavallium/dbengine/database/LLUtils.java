package it.cavallium.dbengine.database;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.CompositeBuffer;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import io.net5.util.IllegalReferenceCountException;
import io.net5.util.internal.PlatformDependent;
import it.cavallium.dbengine.database.collections.DatabaseStage;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDB;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;

@SuppressWarnings("unused")
public class LLUtils {

	private static final Logger logger = LoggerFactory.getLogger(LLUtils.class);
	public static final Marker MARKER_ROCKSDB = MarkerFactory.getMarker("ROCKSDB");
	public static final Marker MARKER_LUCENE = MarkerFactory.getMarker("LUCENE");

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
			case NO_SCORES -> ScoreMode.TOP_DOCS;
		};
	}

	public static Term toTerm(LLTerm term) {
		return new Term(term.getKey(), term.getValue());
	}

	public static Document toDocument(LLUpdateDocument document) {
		Document d = new Document();
		for (LLItem item : document.items()) {
			d.add(LLUtils.toField(item));
		}
		return d;
	}

	public static Field[] toFields(LLItem... fields) {
		Field[] d = new Field[fields.length];
		for (int i = 0; i < fields.length; i++) {
			d[i] = LLUtils.toField(fields[i]);
		}
		return d;
	}

	public static Collection<Document> toDocuments(Collection<LLUpdateDocument> document) {
		List<Document> d = new ArrayList<>(document.size());
		for (LLUpdateDocument doc : document) {
			d.add(LLUtils.toDocument(doc));
		}
		return d;
	}

	public static Collection<Document> toDocumentsFromEntries(Collection<Entry<LLTerm, LLUpdateDocument>> documentsList) {
		ArrayList<Document> results = new ArrayList<>(documentsList.size());
		for (Entry<LLTerm, LLUpdateDocument> entry : documentsList) {
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

	private static Field toField(LLItem item) {
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

	public static String toStringSafe(@Nullable Buffer key) {
		try {
			if (key == null || key.isAccessible()) {
				return toString(key);
			} else {
				return "(released)";
			}
		} catch (IllegalReferenceCountException ex) {
			return "(released)";
		}
	}

	public static String toStringSafe(byte @Nullable[] key) {
		try {
			if (key == null) {
				return toString(key);
			} else {
				return "(released)";
			}
		} catch (IllegalReferenceCountException ex) {
			return "(released)";
		}
	}

	public static String toStringSafe(@Nullable LLRange range) {
		try {
			if (range == null || range.isAccessible()) {
				return toString(range);
			} else {
				return "(released)";
			}
		} catch (IllegalReferenceCountException ex) {
			return "(released)";
		}
	}

	public static String toString(@Nullable LLRange range) {
		if (range == null) {
			return "null";
		} else if (range.isAll()) {
			return "ξ";
		} else if (range.hasMin() && range.hasMax()) {
			return "[" + toStringSafe(range.getMinUnsafe()) + "," + toStringSafe(range.getMaxUnsafe()) + ")";
		} else if (range.hasMin()) {
			return "[" + toStringSafe(range.getMinUnsafe()) + ",*)";
		} else if (range.hasMax()) {
			return "[*," + toStringSafe(range.getMaxUnsafe()) + ")";
		} else {
			return "∅";
		}
	}

	public static String toString(@Nullable Buffer key) {
		if (key == null) {
			return "null";
		} else {
			int startIndex = key.readerOffset();
			int iMax = key.readableBytes() - 1;
			int iLimit = 128;
			if (iMax <= -1) {
				return "[]";
			} else {
				StringBuilder arraySB = new StringBuilder();
				StringBuilder asciiSB = new StringBuilder();
				boolean isAscii = true;
				arraySB.append('[');
				int i = 0;

				while (true) {
					var byteVal = key.getUnsignedByte(startIndex + i);
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
							return arraySB.append(']').toString();
						}
					}

					arraySB.append(", ");
					++i;
				}
			}
		}
	}

	public static String toString(byte @Nullable[] key) {
		if (key == null) {
			return "null";
		} else {
			int startIndex = 0;
			int iMax = key.length - 1;
			int iLimit = 128;
			if (iMax <= -1) {
				return "[]";
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
							return arraySB.append(']').toString();
						}
					}

					arraySB.append(", ");
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

	public static byte[] toArray(@Nullable Buffer key) {
		if (key == null) {
			return EMPTY_BYTE_ARRAY;
		}
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
	public static Buffer readNullableDirectNioBuffer(BufferAllocator alloc, ToIntFunction<ByteBuffer> reader) {
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
						return buffer;
					} else {
						assert size > directBuffer.limit();
						assert directBuffer.limit() > 0;
						// Free the buffer
						if (directBuffer != null) {
							// todo: check if free is needed
							PlatformDependent.freeDirectBuffer(directBuffer);
							directBuffer = null;
						}
						directBuffer = LLUtils.obtainDirect(buffer, true);
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
		}
	}

	public static void ensureBlocking() {
		if (Schedulers.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Called collect in a nonblocking thread");
		}
	}

	// todo: remove this ugly method
	/**
	 * cleanup resource
	 * @param cleanupOnSuccess if true the resource will be cleaned up if the function is successful
	 */
	public static <U, T extends Resource<T>> Mono<U> usingSend(Mono<Send<T>> resourceSupplier,
			Function<Send<T>, Mono<U>> resourceClosure,
			boolean cleanupOnSuccess) {
		return Mono.usingWhen(resourceSupplier, resourceClosure, r -> {
			if (cleanupOnSuccess) {
				return Mono.fromRunnable(() -> r.close());
			} else {
				return Mono.empty();
			}
		}, (r, ex) -> Mono.fromRunnable(() -> r.close()), r -> Mono.fromRunnable(() -> r.close()))
				.doOnDiscard(Send.class, send -> send.close());
	}

	// todo: remove this ugly method
	/**
	 * cleanup resource
	 * @param cleanupOnSuccess if true the resource will be cleaned up if the function is successful
	 */
	public static <U, T extends Resource<T>, V extends T> Mono<U> usingResource(Mono<V> resourceSupplier,
			Function<V, Mono<U>> resourceClosure,
			boolean cleanupOnSuccess) {
		return Mono.usingWhen(resourceSupplier, resourceClosure, r -> {
					if (cleanupOnSuccess) {
						return Mono.fromRunnable(() -> r.close());
					} else {
						return Mono.empty();
					}
				}, (r, ex) -> Mono.fromRunnable(() -> r.close()), r -> Mono.fromRunnable(() -> r.close()))
				.doOnDiscard(Resource.class, resource -> resource.close())
				.doOnDiscard(Send.class, send -> send.close());
	}

	// todo: remove this ugly method
	/**
	 * cleanup resource
	 * @param cleanupOnSuccess if true the resource will be cleaned up if the function is successful
	 */
	public static <U, T extends Resource<T>, V extends T> Flux<U> usingEachResource(Flux<V> resourceSupplier,
			Function<V, Mono<U>> resourceClosure,
			boolean cleanupOnSuccess) {
		return resourceSupplier
				.concatMap(resource -> Mono.usingWhen(Mono.just(resource), resourceClosure, r -> {
					if (cleanupOnSuccess) {
						return Mono.fromRunnable(() -> r.close());
					} else {
						return Mono.empty();
					}
				}, (r, ex) -> Mono.fromRunnable(() -> r.close()), r -> Mono.fromRunnable(() -> r.close())))
				.doOnDiscard(Resource.class, resource -> resource.close())
				.doOnDiscard(Send.class, send -> send.close());
	}

	// todo: remove this ugly method
	/**
	 * cleanup resource
	 * @param cleanupOnSuccess if true the resource will be cleaned up if the function is successful
	 */
	public static <U, T extends Resource<T>> Mono<U> usingSendResource(Mono<Send<T>> resourceSupplier,
			Function<T, Mono<U>> resourceClosure,
			boolean cleanupOnSuccess) {
		return Mono.usingWhen(resourceSupplier.map(Send::receive), resourceClosure, r -> {
					if (cleanupOnSuccess) {
						return Mono.fromRunnable(() -> r.close());
					} else {
						return Mono.empty();
					}
				}, (r, ex) -> Mono.fromRunnable(() -> r.close()), r -> Mono.fromRunnable(() -> r.close()))
				.doOnDiscard(Resource.class, resource -> resource.close())
				.doOnDiscard(Send.class, send -> send.close());
	}

	// todo: remove this ugly method
	/**
	 * cleanup resource
	 * @param cleanupOnSuccess if true the resource will be cleaned up if the function is successful
	 */
	public static <U, T extends Resource<T>> Flux<U> usingSendResources(Mono<Send<T>> resourceSupplier,
			Function<T, Flux<U>> resourceClosure,
			boolean cleanupOnSuccess) {
		return Flux.usingWhen(resourceSupplier.map(Send::receive), resourceClosure, r -> {
					if (cleanupOnSuccess) {
						return Mono.fromRunnable(() -> r.close());
					} else {
						return Mono.empty();
					}
				}, (r, ex) -> Mono.fromRunnable(() -> r.close()), r -> Mono.fromRunnable(() -> r.close()))
				.doOnDiscard(Resource.class, resource -> resource.close())
				.doOnDiscard(Send.class, send -> send.close());
	}

	public static boolean isSet(ScoreDoc[] scoreDocs) {
		for (ScoreDoc scoreDoc : scoreDocs) {
			if (scoreDoc == null) {
				return false;
			}
		}
		return true;
	}

	public static Send<Buffer> empty(BufferAllocator allocator) {
		try {
			return allocator.allocate(0).send();
		} catch (Exception ex) {
			try (var empty = CompositeBuffer.compose(allocator)) {
				assert empty.readableBytes() == 0;
				assert empty.capacity() == 0;
				return empty.send();
			}
		}
	}

	public static Send<Buffer> copy(BufferAllocator allocator, Buffer buf) {
		if (CompositeBuffer.isComposite(buf) && buf.capacity() == 0) {
			return empty(allocator);
		} else {
			return buf.copy().send();
		}
	}

	public static record DirectBuffer(@NotNull Send<Buffer> buffer, @NotNull ByteBuffer byteBuffer) {}

	@NotNull
	public static DirectBuffer newDirect(BufferAllocator allocator, int size) {
		try (var buf = allocator.allocate(size)) {
			var direct = obtainDirect(buf, true);
			return new DirectBuffer(buf.send(), direct);
		}
	}

	@NotNull
	public static DirectBuffer convertToReadableDirect(BufferAllocator allocator, Send<Buffer> content) {
		try (var buf = content.receive()) {
			DirectBuffer result;
			if (buf.countComponents() == 1) {
				var direct = obtainDirect(buf, false);
				result = new DirectBuffer(buf.send(), direct);
			} else {
				var direct = newDirect(allocator, buf.readableBytes());
				try (var buf2 = direct.buffer().receive()) {
					buf.copyInto(buf.readerOffset(), buf2, buf2.writerOffset(), buf.readableBytes());
					buf2.writerOffset(buf2.writerOffset() + buf.readableBytes());
					assert buf2.readableBytes() == buf.readableBytes();
					result = new DirectBuffer(buf2.send(), direct.byteBuffer());
				}
			}
			return result;
		}
	}

	@NotNull
	public static ByteBuffer obtainDirect(Buffer buffer, boolean writable) {
		if (!PlatformDependent.hasUnsafe()) {
			throw new UnsupportedOperationException("Please enable unsafe support or disable netty direct buffers",
					PlatformDependent.getUnsafeUnavailabilityCause()
			);
		}
		if (!MemorySegmentUtils.isSupported()) {
			throw new UnsupportedOperationException("Foreign Memory Access API support is disabled."
					+ " Please set \"" + MemorySegmentUtils.getSuggestedArgs() + "\"",
					MemorySegmentUtils.getUnsupportedCause()
			);
		}
		assert buffer.isAccessible();
		if (buffer.readOnly()) {
			throw new IllegalStateException("Buffer is read only");
		}
		buffer.compact();
		assert buffer.readerOffset() == 0;
		AtomicLong nativeAddress = new AtomicLong(0);
		if (buffer.countComponents() == 1) {
			if (writable) {
				if (buffer.countWritableComponents() == 1) {
					buffer.forEachWritable(0, (i, c) -> {
						assert c.writableNativeAddress() != 0;
						nativeAddress.setPlain(c.writableNativeAddress());
						return false;
					});
				}
			} else {
				var readableComponents = buffer.countReadableComponents();
				if (readableComponents == 1) {
					buffer.forEachReadable(0, (i, c) -> {
						assert c.readableNativeAddress() != 0;
						nativeAddress.setPlain(c.readableNativeAddress());
						return false;
					});
				}
			}
		}
		if (nativeAddress.getPlain() == 0) {
			if (buffer.capacity() == 0) {
				return EMPTY_BYTE_BUFFER;
			}
			if (!buffer.isAccessible()) {
				throw new IllegalStateException("Buffer is not accessible");
			}
			throw new IllegalStateException("Buffer is not direct");
		}
		return MemorySegmentUtils.directBuffer(nativeAddress.getPlain(), writable ? buffer.capacity() : buffer.writerOffset());
	}

	public static Buffer fromByteArray(BufferAllocator alloc, byte[] array) {
		Buffer result = alloc.allocate(array.length);
		result.writeBytes(array);
		return result;
	}

	@NotNull
	public static Buffer readDirectNioBuffer(BufferAllocator alloc, ToIntFunction<ByteBuffer> reader) {
		var nullable = readNullableDirectNioBuffer(alloc, reader);
		if (nullable == null) {
			throw new IllegalStateException("A non-nullable buffer read operation tried to return a \"not found\" element");
		}
		return nullable;
	}

	public static Buffer compositeBuffer(BufferAllocator alloc, Send<Buffer> buffer) {
		return buffer.receive();
	}

	@NotNull
	public static Buffer compositeBuffer(BufferAllocator alloc,
			@NotNull Send<Buffer> buffer1,
			@NotNull Send<Buffer> buffer2) {
		var b1 = buffer1.receive();
		try (var b2 = buffer2.receive()) {
			if (b1.writerOffset() < b1.capacity() || b2.writerOffset() < b2.capacity()) {
				b1.ensureWritable(b2.readableBytes(), b2.readableBytes(), true);
				b2.copyInto(b2.readerOffset(), b1, b1.writerOffset(), b2.readableBytes());
				b1.writerOffset(b1.writerOffset() + b2.readableBytes());
				return b1;
			} else {
				return CompositeBuffer.compose(alloc, b1.send(), b2.send());
			}
		}
	}

	@NotNull
	public static Buffer compositeBuffer(BufferAllocator alloc,
			@NotNull Send<Buffer> buffer1,
			@NotNull Send<Buffer> buffer2,
			@NotNull Send<Buffer> buffer3) {
		var b1 = buffer1.receive();
		try (var b2 = buffer2.receive()) {
			try (var b3 = buffer3.receive()) {
				if (b1.writerOffset() < b1.capacity()
						|| b2.writerOffset() < b2.capacity()
						|| b3.writerOffset() < b3.capacity()) {
					b1.ensureWritable(b2.readableBytes(), b2.readableBytes(), true);
					b2.copyInto(b2.readerOffset(), b1, b1.writerOffset(), b2.readableBytes());
					b1.writerOffset(b1.writerOffset() + b2.readableBytes());

					b1.ensureWritable(b3.readableBytes(), b3.readableBytes(), true);
					b3.copyInto(b3.readerOffset(), b1, b1.writerOffset(), b3.readableBytes());
					b1.writerOffset(b1.writerOffset() + b3.readableBytes());
					return b1;
				} else {
					return CompositeBuffer.compose(alloc, b1.send(), b2.send(), b3.send());
				}
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

	public static Mono<Send<Buffer>> resolveLLDelta(Mono<Send<LLDelta>> prev, UpdateReturnMode updateReturnMode) {
		return prev.handle((deltaToReceive, sink) -> {
			try (var delta = deltaToReceive.receive()) {
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

	public static <U> Mono<Delta<U>> mapLLDelta(Mono<Send<LLDelta>> mono,
			SerializationFunction<@NotNull Send<Buffer>, @Nullable U> mapper) {
		return mono.handle((deltaToReceive, sink) -> {
			try (var delta = deltaToReceive.receive()) {
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
			} else if (obj instanceof LLDelta o) {
				discardLLDelta(o);
			} else if (obj instanceof Delta o) {
				discardDelta(o);
			} else if (obj instanceof Send o) {
				discardSend(o);
			} else if (obj instanceof Map o) {
				discardMap(o);
			} else if (obj instanceof DatabaseStage o) {
				discardStage(o);
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
				.doOnDiscard(LLDelta.class, LLUtils::discardLLDelta)
				.doOnDiscard(Send.class, LLUtils::discardSend)
				.doOnDiscard(Map.class, LLUtils::discardMap)
				.doOnDiscard(DatabaseStage.class, LLUtils::discardStage);

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
			} else if (obj instanceof LLDelta o) {
				discardLLDelta(o);
			} else if (obj instanceof Send o) {
				discardSend(o);
			} else if (obj instanceof Map o) {
				discardMap(o);
			} else if (obj instanceof DatabaseStage o) {
				discardStage(o);
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
				.doOnDiscard(LLDelta.class, LLUtils::discardLLDelta)
				.doOnDiscard(Send.class, LLUtils::discardSend)
				.doOnDiscard(Map.class, LLUtils::discardMap)
				.doOnDiscard(DatabaseStage.class, LLUtils::discardStage);

		 */
	}

	private static void discardLLEntry(LLEntry entry) {
		logger.trace(MARKER_ROCKSDB, "Releasing discarded Buffer");
		entry.close();
	}

	private static void discardLLRange(LLRange range) {
		logger.trace(MARKER_ROCKSDB, "Releasing discarded LLRange");
		range.close();
	}

	private static void discardLLDelta(LLDelta delta) {
		logger.trace(MARKER_ROCKSDB, "Releasing discarded LLDelta");
		delta.close();
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

	private static void discardStage(DatabaseStage<?> stage) {
		// do nothing for now, to avoid double-free problems
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

	public static String deserializeString(@NotNull Buffer buffer, int readerOffset, int length, Charset charset) {
		byte[] bytes = new byte[Math.min(length, buffer.readableBytes())];
		buffer.copyInto(readerOffset, bytes, 0, length);
		return new String(bytes, charset);
	}

	public static int utf8MaxBytes(String deserialized) {
		return deserialized.length() * 3;
	}
}
