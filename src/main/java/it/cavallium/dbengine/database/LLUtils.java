package it.cavallium.dbengine.database;

import static io.net5.buffer.api.StandardAllocationTypes.OFF_HEAP;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.net5.buffer.api.AllocatorControl;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.CompositeBuffer;
import io.net5.buffer.api.MemoryManager;
import io.net5.buffer.api.ReadableComponent;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.WritableComponent;
import io.net5.buffer.api.bytebuffer.ByteBufferMemoryManager;
import io.net5.buffer.api.internal.Statics;
import io.net5.buffer.api.unsafe.UnsafeMemoryManager;
import io.net5.util.IllegalReferenceCountException;
import it.cavallium.dbengine.database.disk.UpdateAtomicResultCurrent;
import it.cavallium.dbengine.database.disk.UpdateAtomicResultDelta;
import it.cavallium.dbengine.database.disk.UpdateAtomicResultPrevious;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.lucene.RandomSortField;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDB;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@SuppressWarnings("unused")
public class LLUtils {

	private static final Logger logger = LogManager.getLogger(LLUtils.class);
	public static final Marker MARKER_ROCKSDB = MarkerManager.getMarker("ROCKSDB");
	public static final Marker MARKER_LUCENE = MarkerManager.getMarker("LUCENE");

	public static final int INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES = 4096;
	public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocateDirect(0).asReadOnlyBuffer();
	@Nullable
	private static final MemoryManager UNSAFE_MEMORY_MANAGER;
	private static final AllocatorControl NO_OP_ALLOCATION_CONTROL = (AllocatorControl) BufferAllocator.offHeapUnpooled();
	private static final byte[] RESPONSE_TRUE = new byte[]{1};
	private static final byte[] RESPONSE_FALSE = new byte[]{0};
	private static final byte[] RESPONSE_TRUE_BUF = new byte[]{1};
	private static final byte[] RESPONSE_FALSE_BUF = new byte[]{0};
	public static final byte[][] LEXICONOGRAPHIC_ITERATION_SEEKS = new byte[256][1];
	public static final AtomicBoolean hookRegistered = new AtomicBoolean();

	static {
		MemoryManager unsafeMemoryManager;
		try {
			unsafeMemoryManager = new UnsafeMemoryManager();
		} catch (UnsupportedOperationException ignored) {
			unsafeMemoryManager = new ByteBufferMemoryManager();
		}
		UNSAFE_MEMORY_MANAGER = unsafeMemoryManager;
		for (int i1 = 0; i1 < 256; i1++) {
			var b = LEXICONOGRAPHIC_ITERATION_SEEKS[i1];
			b[0] = (byte) i1;
		}
		initHooks();
	}

	public static void initHooks() {
		if (hookRegistered.compareAndSet(false, true)) {
			Hooks.onNextDropped(LLUtils::onNextDropped);
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
		var valueRef = new FakeBytesRefBuilder(term);
		return new Term(term.getKey(), valueRef);
	}

	public static Document toDocument(LLUpdateDocument document) {
		return toDocument(document.items());
	}

	public static Document toDocument(LLItem[] document) {
		Document d = new Document();
		for (LLItem item : document) {
			if (item != null) {
				d.add(LLUtils.toField(item));
			}
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
			case IntPoint -> new IntPoint(item.getName(), item.intData());
			case DoublePoint -> new DoublePoint(item.getName(), item.doubleData());
			case IntPointND -> new IntPoint(item.getName(), item.intArrayData());
			case LongPoint -> new LongPoint(item.getName(), item.longData());
			case LongPointND -> new LongPoint(item.getName(), item.longArrayData());
			case FloatPointND -> new FloatPoint(item.getName(), item.floatArrayData());
			case DoublePointND -> new DoublePoint(item.getName(), item.doubleArrayData());
			case LongStoredField -> new StoredField(item.getName(), item.longData());
			case BytesStoredField -> new StoredField(item.getName(), (BytesRef) item.getData());
			case FloatPoint -> new FloatPoint(item.getName(), item.floatData());
			case TextField -> new TextField(item.getName(), item.stringValue(), Store.NO);
			case TextFieldStored -> new TextField(item.getName(), item.stringValue(), Store.YES);
			case SortedNumericDocValuesField -> new SortedNumericDocValuesField(item.getName(), item.longData());
			case NumericDocValuesField -> new NumericDocValuesField(item.getName(), item.longData());
			case StringField -> new StringField(item.getName(), item.stringValue(), Store.NO);
			case StringFieldStored -> {
				if (item.getData() instanceof BytesRef bytesRef) {
					yield new StringField(item.getName(), bytesRef, Store.YES);
				} else {
					yield new StringField(item.getName(), item.stringValue(), Store.YES);
				}
			}
		};
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

	public static it.cavallium.dbengine.database.LLKeyScore toKeyScore(LLKeyScore hit) {
		return new it.cavallium.dbengine.database.LLKeyScore(hit.docId(), hit.shardId(), hit.score(), hit.key());
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
	@Nullable
	public static Buffer readNullableDirectNioBuffer(BufferAllocator alloc, ToIntFunction<ByteBuffer> reader) {
		var directBuffer = allocateShared(INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES);
		assert directBuffer.readerOffset() == 0;
		assert directBuffer.writerOffset() == 0;
		var directBufferWriter = ((WritableComponent) directBuffer).writableBuffer();
		assert directBufferWriter.position() == 0;
		assert directBufferWriter.isDirect();
		try {
			int trueSize = reader.applyAsInt(directBufferWriter);
			if (trueSize == RocksDB.NOT_FOUND) {
				directBuffer.close();
				return null;
			}
			int readSize = directBufferWriter.limit();
			if (trueSize < readSize) {
				throw new IllegalStateException();
			} else if (trueSize == readSize) {
				return directBuffer.writerOffset(directBufferWriter.limit());
			} else {
				assert directBuffer.readerOffset() == 0;
				directBuffer.ensureWritable(trueSize);
				assert directBuffer.writerOffset() == 0;
				directBufferWriter = ((WritableComponent) directBuffer).writableBuffer();
				assert directBufferWriter.position() == 0;
				assert directBufferWriter.isDirect();
				reader.applyAsInt(directBufferWriter.position(0));
				return directBuffer.writerOffset(trueSize);
			}
		} catch (Throwable t) {
			directBuffer.close();
			throw t;
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
		}, (r, ex) -> Mono.fromRunnable(() -> r.close()), r -> Mono.fromRunnable(() -> r.close()));
	}

	// todo: remove this ugly method
	/**
	 * cleanup resource
	 * @param cleanupOnSuccess if true the resource will be cleaned up if the function is successful
	 */
	public static <U, T extends Resource<? extends T>, V extends T> Mono<U> usingResource(Mono<V> resourceSupplier,
			Function<V, Mono<U>> resourceClosure,
			boolean cleanupOnSuccess) {
		return Mono.usingWhen(resourceSupplier, resourceClosure, r -> {
					if (cleanupOnSuccess) {
						return Mono.fromRunnable(() -> {
							if (r.isAccessible()) {
								r.close();
							}
						});
					} else {
						return Mono.empty();
					}
				}, (r, ex) -> Mono.fromRunnable(() -> {
					if (r.isAccessible()) {
						r.close();
					}
				}), r -> Mono.fromRunnable(() -> {
					if (r.isAccessible()) {
						r.close();
					}
				}));
	}

	// todo: remove this ugly method
	/**
	 * cleanup resource
	 * @param cleanupOnSuccess if true the resource will be cleaned up if the function is successful
	 */
	public static <U, T extends Resource<T>, V extends T> Flux<U> usingResources(Mono<V> resourceSupplier,
			Function<V, Flux<U>> resourceClosure,
			boolean cleanupOnSuccess) {
		return Flux.usingWhen(resourceSupplier, resourceClosure, r -> {
					if (cleanupOnSuccess) {
						return Mono.fromRunnable(() -> {
							if (r.isAccessible()) {
								r.close();
							}
						});
					} else {
						return Mono.empty();
					}
				}, (r, ex) -> Mono.fromRunnable(() -> {
					if (r.isAccessible()) {
						r.close();
					}
				}), r -> Mono.fromRunnable(() -> {
					if (r.isAccessible()) {
						r.close();
					}
				}));
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
						return Mono.fromRunnable(() -> {
							if (r.isAccessible()) {
								r.close();
							}
						});
					} else {
						return Mono.empty();
					}
				}, (r, ex) -> Mono.fromRunnable(() -> {
					if (r.isAccessible()) {
						r.close();
					}
				}), r -> Mono.fromRunnable(() -> {
					if (r.isAccessible()) {
						r.close();
					}
				})));
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
				}, (r, ex) -> Mono.fromRunnable(() -> {
					if (r.isAccessible()) {
						r.close();
					}
				}), r -> Mono.fromRunnable(() -> {
					if (r.isAccessible()) {
						r.close();
					}
				}));
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
						return Mono.fromRunnable(() -> {
							if (r.isAccessible()) {
								r.close();
							}
						});
					} else {
						return Mono.empty();
					}
				}, (r, ex) -> Mono.fromRunnable(() -> {
					if (r.isAccessible()) {
						r.close();
					}
				}), r -> Mono.fromRunnable(() -> {
					if (r.isAccessible()) {
						r.close();
					}
				}));
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

	@Deprecated
	public record DirectBuffer(@NotNull Buffer buffer, @NotNull ByteBuffer byteBuffer) {}

	@NotNull
	public static ByteBuffer newDirect(int size) {
		return ByteBuffer.allocateDirect(size);
	}

	/**
	 * The returned object will be also of type {@link WritableComponent} {@link ReadableComponent}
	 */
	public static Buffer allocateShared(int size) {
		return LLUtils.UNSAFE_MEMORY_MANAGER.allocateShared(NO_OP_ALLOCATION_CONTROL, size, Statics.NO_OP_DROP, OFF_HEAP);
	}

	/**
	 * Get the internal byte buffer, if present
	 */
	@Nullable
	public static ByteBuffer asReadOnlyDirect(Buffer inputBuffer) {
		var bytes = inputBuffer.readableBytes();
		if (bytes == 0) {
			return EMPTY_BYTE_BUFFER;
		}
		if (inputBuffer instanceof ReadableComponent rc) {
			var componentBuffer = rc.readableBuffer();
			if (componentBuffer != null && componentBuffer.isDirect()) {
				assert componentBuffer.isReadOnly();
				assert componentBuffer.isDirect();
				return componentBuffer;
			}
		} else if (inputBuffer.countReadableComponents() == 1) {
			AtomicReference<ByteBuffer> bufferRef = new AtomicReference<>();
			inputBuffer.forEachReadable(0, (index, comp) -> {
				var compBuffer = comp.readableBuffer();
				if (compBuffer != null && compBuffer.isDirect()) {
					bufferRef.setPlain(compBuffer);
				}
				return false;
			});
			var buffer = bufferRef.getPlain();
			if (buffer != null) {
				assert buffer.isReadOnly();
				assert buffer.isDirect();
				return buffer;
			}
		}

		return null;
	}

	/**
	 * Copy the buffer into a newly allocated direct buffer
	 */
	@NotNull
	public static ByteBuffer copyToNewDirectBuffer(Buffer inputBuffer) {
		int bytes = inputBuffer.readableBytes();
		var directBuffer = ByteBuffer.allocateDirect(bytes);
		inputBuffer.copyInto(inputBuffer.readerOffset(), directBuffer, 0, bytes);
		return directBuffer.asReadOnlyBuffer();
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
		return Mono.fromSupplier(() -> {
			if (buf != null && buf.isAccessible()) {
				return buf.copy().send();
			} else {
				return null;
			}
		});
	}

	public static Mono<Send<LLRange>> lazyRetainRange(LLRange range) {
		return Mono.fromSupplier(() -> {
			if (range != null && range.isAccessible()) {
				return range.copy().send();
			} else {
				return null;
			}
		});
	}

	public static Mono<Send<Buffer>> lazyRetain(Callable<Send<Buffer>> bufCallable) {
		return Mono.fromCallable(bufCallable);
	}

	public static Mono<Send<LLRange>> lazyRetainRange(Callable<Send<LLRange>> rangeCallable) {
		return Mono.fromCallable(rangeCallable);
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

	private static void onNextDropped(Object next) {
		if (next instanceof Send<?> send) {
			send.close();
		} else if (next instanceof Resource<?> resource) {
			resource.close();
		} else if (next instanceof Iterable<?> iterable) {
			iterable.forEach(LLUtils::onNextDropped);
		} else if (next instanceof SafeCloseable safeCloseable) {
			safeCloseable.close();
		} else if (next instanceof UpdateAtomicResultDelta delta) {
			delta.delta().close();
		} else if (next instanceof UpdateAtomicResultCurrent cur) {
			cur.current().close();
		} else if (next instanceof UpdateAtomicResultPrevious cur) {
			cur.previous().close();
		} else if (next instanceof Optional<?> optional) {
			optional.ifPresent(LLUtils::onNextDropped);
		} else if (next instanceof Map.Entry<?, ?> entry) {
			var key = entry.getKey();
			if (key != null) {
				onNextDropped(key);
			}
			var value = entry.getValue();
			if (value != null) {
				onNextDropped(value);
			}
		} else if (next instanceof Delta<?> delta) {
			var previous = delta.previous();
			if (previous != null) {
				onNextDropped(previous);
			}
			var current = delta.current();
			if (current != null) {
				onNextDropped(current);
			}
		} else if (next instanceof Map<?, ?> map) {
			map.forEach((key, value) -> {
				if (key != null) {
					onNextDropped(key);
				}
				if (value != null) {
					onNextDropped(value);
				}
			});
		}
	}

	private static class FakeBytesRefBuilder extends BytesRefBuilder {

		private final LLTerm term;

		public FakeBytesRefBuilder(LLTerm term) {
			this.term = term;
		}

		@Override
		public BytesRef toBytesRef() {
			return term.getValueBytesRef();
		}
	}
}
