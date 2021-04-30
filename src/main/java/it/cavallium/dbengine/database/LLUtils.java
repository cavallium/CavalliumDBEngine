package it.cavallium.dbengine.database;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import it.cavallium.dbengine.lucene.RandomSortField;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
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

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.buffer.Unpooled.wrappedBuffer;

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
		return wrappedBuffer(booleanToResponse(bool));
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
		switch (scoreMode) {
			case COMPLETE:
				return ScoreMode.COMPLETE;
			case TOP_SCORES:
				return ScoreMode.TOP_SCORES;
			case COMPLETE_NO_SCORES:
				return ScoreMode.COMPLETE_NO_SCORES;
			default:
				throw new IllegalStateException("Unexpected value: " + scoreMode);
		}
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

	public static Iterable<Document> toDocuments(Iterable<LLDocument> document) {
		List<Document> d = new LinkedList<>();
		for (LLDocument doc : document) {
			d.add(LLUtils.toDocument(doc));
		}
		return d;
	}

	public static Iterable<Term> toTerms(Iterable<LLTerm> terms) {
		List<Term> d = new LinkedList<>();
		for (LLTerm term : terms) {
			d.add(LLUtils.toTerm(term));
		}
		return d;
	}

	private static IndexableField toField(LLItem item) {
		switch (item.getType()) {
			case IntPoint:
				return new IntPoint(item.getName(), Ints.fromByteArray(item.getData()));
			case LongPoint:
				return new LongPoint(item.getName(), Longs.fromByteArray(item.getData()));
			case FloatPoint:
				return new FloatPoint(item.getName(), ByteBuffer.wrap(item.getData()).getFloat());
			case TextField:
				return new TextField(item.getName(), item.stringValue(), Field.Store.NO);
			case TextFieldStored:
				return new TextField(item.getName(), item.stringValue(), Field.Store.YES);
			case SortedNumericDocValuesField:
				return new SortedNumericDocValuesField(item.getName(), Longs.fromByteArray(item.getData()));
			case StringField:
				return new StringField(item.getName(), item.stringValue(), Field.Store.NO);
			case StringFieldStored:
				return new StringField(item.getName(), item.stringValue(), Field.Store.YES);
		}
		throw new UnsupportedOperationException("Unsupported field type");
	}

	public static it.cavallium.dbengine.database.LLKeyScore toKeyScore(LLKeyScore hit) {
		return new it.cavallium.dbengine.database.LLKeyScore(hit.getKey(), hit.getScore());
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
		byte[] keyBytes = new byte[key.readableBytes()];
		key.getBytes(key.readerIndex(), keyBytes, 0, key.readableBytes());
		return keyBytes;
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
		try {
			ByteBuf directBuffer = null;
			ByteBuffer nioBuffer;
			int size;
			Boolean mustBeCopied = null;
			do {
				if (mustBeCopied == null || !mustBeCopied) {
					nioBuffer = LLUtils.toDirectFast(buffer.retain());
					if (nioBuffer != null) {
						nioBuffer.limit(nioBuffer.capacity());
					}
				} else {
					nioBuffer = null;
				}
				if ((mustBeCopied != null && mustBeCopied) || nioBuffer == null) {
					directBuffer = LLUtils.toDirectCopy(buffer.retain());
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
		} catch (Throwable t) {
			buffer.release();
			throw t;
		}
		return null;
	}

	@Nullable
	public static ByteBuffer toDirectFast(ByteBuf buffer) {
		try {
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
		} finally {
			buffer.release();
		}
	}

	public static ByteBuf toDirectCopy(ByteBuf buffer) {
		try {
			ByteBuf directCopyBuf = buffer.alloc().directBuffer(buffer.capacity(), buffer.maxCapacity());
			directCopyBuf.writeBytes(buffer, 0, buffer.writerIndex());
			return directCopyBuf;
		} finally {
			buffer.release();
		}
	}

	public static ByteBuf convertToDirectByteBuf(AbstractByteBufAllocator alloc, ByteBuf buffer) {
		ByteBuf result;
		ByteBuf directCopyBuf = alloc.directBuffer(buffer.capacity(), buffer.maxCapacity());
		directCopyBuf.writeBytes(buffer, 0, buffer.writerIndex());
		directCopyBuf.readerIndex(buffer.readerIndex());
		result = directCopyBuf;
		assert result.isDirect();
		assert result.capacity() == buffer.capacity();
		assert buffer.readerIndex() == result.readerIndex();
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

	public static ByteBuf directCompositeBuffer(ByteBufAllocator alloc, ByteBuf buffer) {
		return wrappedBuffer(buffer);
	}

	public static ByteBuf directCompositeBuffer(ByteBufAllocator alloc, ByteBuf buffer1, ByteBuf buffer2) {
		assert buffer1.isDirect();
		assert buffer1.nioBuffer().isDirect();
		assert buffer2.isDirect();
		assert buffer2.nioBuffer().isDirect();
		if (buffer1.readableBytes() == 0) {
			return wrappedBuffer(buffer2);
		} else if (buffer2.readableBytes() == 0) {
			return wrappedBuffer(buffer1);
		}
		CompositeByteBuf compositeBuffer = alloc.compositeDirectBuffer(2);
		compositeBuffer.addComponent(true, buffer1);
		compositeBuffer.addComponent(true, buffer2);
		compositeBuffer.consolidate();
		assert compositeBuffer.isDirect();
		assert compositeBuffer.nioBuffer().isDirect();
		return compositeBuffer;
	}

	public static ByteBuf directCompositeBuffer(ByteBufAllocator alloc, ByteBuf buffer1, ByteBuf buffer2, ByteBuf buffer3) {
		if (buffer1.readableBytes() == 0) {
			return directCompositeBuffer(alloc, buffer2, buffer3);
		} else if (buffer2.readableBytes() == 0) {
			return directCompositeBuffer(alloc, buffer1, buffer3);
		} else if (buffer3.readableBytes() == 0) {
			return directCompositeBuffer(alloc, buffer1, buffer2);
		}
		CompositeByteBuf compositeBuffer = alloc.compositeDirectBuffer(3);
		compositeBuffer.addComponent(true, buffer1);
		compositeBuffer.addComponent(true, buffer2);
		compositeBuffer.addComponent(true, buffer3);
		compositeBuffer.consolidate();
		return compositeBuffer;
	}

	public static ByteBuf directCompositeBuffer(ByteBufAllocator alloc, ByteBuf... buffers) {
		switch (buffers.length) {
			case 0:
				return EMPTY_BUFFER;
			case 1:
				return directCompositeBuffer(alloc, buffers[0]);
			case 2:
				return directCompositeBuffer(alloc, buffers[0], buffers[1]);
			case 3:
				return directCompositeBuffer(alloc, buffers[0], buffers[1], buffers[2]);
			default:
				CompositeByteBuf compositeBuffer = alloc.compositeDirectBuffer(buffers.length);
				compositeBuffer.addComponents(true, buffers);
				compositeBuffer.consolidate();
				return compositeBuffer;
		}
	}
}
