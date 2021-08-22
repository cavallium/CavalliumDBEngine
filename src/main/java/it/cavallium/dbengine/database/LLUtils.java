package it.cavallium.dbengine.database;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.database.disk.ReleasableSlice;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.lucene.RandomSortField;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Callable;
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
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDB;
import org.warp.commonutils.functional.IOFunction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class LLUtils {

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

	public static boolean responseToBoolean(ByteBuf response) {
		try {
			assert response.readableBytes() == 1;
			return response.getByte(response.readerIndex()) == 1;
		} finally {
			response.release();
		}
	}

	public static byte[] booleanToResponse(boolean bool) {
		return bool ? RESPONSE_TRUE : RESPONSE_FALSE;
	}

	public static ByteBuf booleanToResponseByteBuffer(boolean bool) {
		return Unpooled.wrappedBuffer(booleanToResponse(bool));
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

	public static String toStringSafe(ByteBuf key) {
		try {
			if (key.refCnt() > 0) {
				return toString(key);
			} else {
				return "(released)";
			}
		} catch (IllegalReferenceCountException ex) {
			return "(released)";
		}
	}

	public static String toString(ByteBuf key) {
		if (key == null) {
			return "null";
		} else {
			int startIndex = key.readerIndex();
			int iMax = key.readableBytes() - 1;
			int iLimit = 128;
			if (iMax <= -1) {
				return "[]";
			} else {
				StringBuilder b = new StringBuilder();
				b.append('[');
				int i = 0;

				while(true) {
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

	public static boolean equals(ByteBuf a, ByteBuf b) {
		if (a == null && b == null) {
			return true;
		} else if (a != null && b != null) {
			return ByteBufUtil.equals(a, b);
		} else {
			return false;
		}
	}

	public static byte[] toArray(ByteBuf key) {
		if (key.hasArray()) {
			return Arrays.copyOfRange(key.array(), key.arrayOffset() + key.readerIndex(), key.arrayOffset() + key.writerIndex());
		} else {
			byte[] keyBytes = new byte[key.readableBytes()];
			key.getBytes(key.readerIndex(), keyBytes, 0, key.readableBytes());
			return keyBytes;
		}
	}

	public static List<byte[]> toArray(List<ByteBuf> input) {
		List<byte[]> result = new ArrayList<>(input.size());
		for (ByteBuf byteBuf : input) {
			result.add(toArray(byteBuf));
		}
		return result;
	}

	public static int hashCode(ByteBuf buf) {
		return buf == null ? 0 : buf.hashCode();
	}

	@Nullable
	public static ByteBuf readNullableDirectNioBuffer(ByteBufAllocator alloc, ToIntFunction<ByteBuffer> reader) {
		ByteBuf buffer = alloc.directBuffer();
		ByteBuf directBuffer = null;
		ByteBuffer nioBuffer;
		int size;
		Boolean mustBeCopied = null;
		do {
			if (mustBeCopied == null || !mustBeCopied) {
				nioBuffer = LLUtils.toDirectFast(buffer);
				if (nioBuffer != null) {
					nioBuffer.limit(nioBuffer.capacity());
				}
			} else {
				nioBuffer = null;
			}
			if ((mustBeCopied != null && mustBeCopied) || nioBuffer == null) {
				directBuffer = buffer;
				nioBuffer = directBuffer.nioBuffer(0, directBuffer.capacity());
				mustBeCopied = true;
			} else {
				mustBeCopied = false;
			}
			try {
				assert nioBuffer.isDirect();
				size = reader.applyAsInt(nioBuffer);
				if (size != RocksDB.NOT_FOUND) {
					if (mustBeCopied) {
						buffer.writerIndex(0).writeBytes(nioBuffer);
					}
					if (size == nioBuffer.limit()) {
						buffer.setIndex(0, size);
						return buffer;
					} else {
						assert size > nioBuffer.limit();
						assert nioBuffer.limit() > 0;
						buffer.capacity(size);
					}
				}
			} finally {
				if (nioBuffer != null) {
					nioBuffer = null;
				}
				if(directBuffer != null) {
					directBuffer.release();
					directBuffer = null;
				}
			}
		} while (size != RocksDB.NOT_FOUND);
		return null;
	}

	@Nullable
	public static ByteBuffer toDirectFast(ByteBuf buffer) {
		ByteBuffer result = buffer.nioBuffer(0, buffer.capacity());
		if (result.isDirect()) {
			result.limit(buffer.writerIndex());

			assert result.isDirect();
			assert result.capacity() == buffer.capacity();
			assert buffer.readerIndex() == result.position();
			assert result.limit() - result.position() == buffer.readableBytes();

			return result;
		} else {
			return null;
		}
	}

	public static ByteBuffer toDirect(ByteBuf buffer) {
		ByteBuffer result = toDirectFast(buffer);
		if (result == null) {
			throw new IllegalArgumentException("The supplied ByteBuf is not direct "
					+ "(if it's a CompositeByteBuf it must be consolidated before)");
		}
		assert result.isDirect();
		return result;
	}

	/*
	public static ByteBuf toDirectCopy(ByteBuf buffer) {
		try {
			ByteBuf directCopyBuf = buffer.alloc().buffer(buffer.capacity(), buffer.maxCapacity());
			directCopyBuf.writeBytes(buffer, 0, buffer.writerIndex());
			return directCopyBuf;
		} finally {
			buffer.release();
		}
	}
	 */

	public static ByteBuf convertToDirectByteBuf(ByteBufAllocator alloc, ByteBuf buffer) {
		ByteBuf result;
		ByteBuf directCopyBuf = alloc.buffer(buffer.capacity(), buffer.maxCapacity());
		directCopyBuf.writeBytes(buffer, 0, buffer.writerIndex());
		directCopyBuf.readerIndex(buffer.readerIndex());
		result = directCopyBuf;
		assert result.isDirect();
		assert result.capacity() == buffer.capacity();
		assert buffer.readerIndex() == result.readerIndex();
		return result;
	}

	public static ByteBuf fromByteArray(ByteBufAllocator alloc, byte[] array) {
		ByteBuf result = alloc.buffer(array.length);
		result.writeBytes(array);
		return result;
	}

	@NotNull
	public static ByteBuf readDirectNioBuffer(ByteBufAllocator alloc, ToIntFunction<ByteBuffer> reader) {
		var buffer = readNullableDirectNioBuffer(alloc, reader);
		if (buffer == null) {
			throw new IllegalStateException("A non-nullable buffer read operation tried to return a \"not found\" element");
		}
		return buffer;
	}

	public static ByteBuf compositeBuffer(ByteBufAllocator alloc, ByteBuf buffer) {
		return buffer;
	}

	public static ByteBuf compositeBuffer(ByteBufAllocator alloc, ByteBuf buffer1, ByteBuf buffer2) {
		try {
			if (buffer1.readableBytes() == 0) {
				return compositeBuffer(alloc, buffer2.retain());
			} else if (buffer2.readableBytes() == 0) {
				return compositeBuffer(alloc, buffer1.retain());
			}
			CompositeByteBuf result = alloc.compositeBuffer(2);
			try {
				result.addComponent(true, buffer1.retain());
				result.addComponent(true, buffer2.retain());
				return result.consolidate().retain();
			} finally {
				result.release();
			}
		} finally {
			buffer1.release();
			buffer2.release();
		}
	}

	public static ByteBuf compositeBuffer(ByteBufAllocator alloc, ByteBuf buffer1, ByteBuf buffer2, ByteBuf buffer3) {
		try {
			if (buffer1.readableBytes() == 0) {
				return compositeBuffer(alloc, buffer2.retain(), buffer3.retain());
			} else if (buffer2.readableBytes() == 0) {
				return compositeBuffer(alloc, buffer1.retain(), buffer3.retain());
			} else if (buffer3.readableBytes() == 0) {
				return compositeBuffer(alloc, buffer1.retain(), buffer2.retain());
			}
			CompositeByteBuf result = alloc.compositeBuffer(3);
			try {
				result.addComponent(true, buffer1.retain());
				result.addComponent(true, buffer2.retain());
				result.addComponent(true, buffer3.retain());
				return result.consolidate().retain();
			} finally {
				result.release();
			}
		} finally {
			buffer1.release();
			buffer2.release();
			buffer3.release();
		}
	}

	public static ByteBuf compositeBuffer(ByteBufAllocator alloc, ByteBuf... buffers) {
		try {
			switch (buffers.length) {
				case 0:
					return alloc.buffer(0);
				case 1:
					return compositeBuffer(alloc, buffers[0].retain().retain());
				case 2:
					return compositeBuffer(alloc, buffers[0].retain(), buffers[1].retain());
				case 3:
					return compositeBuffer(alloc, buffers[0].retain(), buffers[1].retain(), buffers[2].retain());
				default:
					CompositeByteBuf result = alloc.compositeBuffer(buffers.length);
					try {
						for (ByteBuf buffer : buffers) {
							result.addComponent(true, buffer.retain());
						}
						return result.consolidate().retain();
					} finally {
						result.release();
					}
			}
		} finally {
			for (ByteBuf buffer : buffers) {
				buffer.release();
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

	public static <R, V> boolean isDeltaChanged(Delta<V> delta) {
		return !Objects.equals(delta.previous(), delta.current());
	}

	public static Mono<ByteBuf> lazyRetain(ByteBuf buf) {
		return Mono.just(buf).map(ByteBuf::retain);
	}

	public static Mono<LLRange> lazyRetainRange(LLRange range) {
		return Mono.just(range).map(LLRange::retain);
	}

	public static Mono<ByteBuf> lazyRetain(Callable<ByteBuf> bufCallable) {
		return Mono.fromCallable(bufCallable).cacheInvalidateIf(byteBuf -> {
			// Retain if the value has been cached previously
			byteBuf.retain();
			return false;
		});
	}

	public static Mono<LLRange> lazyRetainRange(Callable<LLRange> rangeCallable) {
		return Mono.fromCallable(rangeCallable).cacheInvalidateIf(range -> {
			// Retain if the value has been cached previously
			range.retain();
			return false;
		});
	}

	public static <T> Mono<T> handleDiscard(Mono<T> mono) {
		return mono.doOnDiscard(Map.Entry.class, e -> {
			if (e.getKey() instanceof ByteBuf bb) {
				if (bb.refCnt() > 0) {
					bb.release();
				}
			}
			if (e.getValue() instanceof ByteBuf bb) {
				if (bb.refCnt() > 0) {
					bb.release();
				}
			}
		});
	}

	public static <T> Flux<T> handleDiscard(Flux<T> mono) {
		return mono
				.doOnDiscard(ReferenceCounted.class, LLUtils::discardRefCounted)
				.doOnDiscard(Map.Entry.class, LLUtils::discardEntry);
	}

	private static void discardEntry(Map.Entry<?, ?> e) {
		if (e.getKey() instanceof ByteBuf bb) {
			if (bb.refCnt() > 0) {
				bb.release();
			}
		}
		if (e.getValue() instanceof ByteBuf bb) {
			if (bb.refCnt() > 0) {
				bb.release();
			}
		}
	}

	private static void discardRefCounted(ReferenceCounted referenceCounted) {
		if (referenceCounted.refCnt() > 0) {
			referenceCounted.release();
		}
	}
}
