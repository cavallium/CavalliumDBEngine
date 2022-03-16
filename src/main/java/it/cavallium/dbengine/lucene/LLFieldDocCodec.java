package it.cavallium.dbengine.lucene;

import io.netty5.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.function.Function;

public class LLFieldDocCodec implements LMDBSortedCodec<LLFieldDoc> {

	private enum FieldType {
		FLOAT,
		DOUBLE,
		INT,
		LONG;

		public byte ordinalByte() {
			return (byte) ordinal();
		}
	}

	@Override
	public ByteBuf serialize(Function<Integer, ByteBuf> allocator, LLFieldDoc data) {
		int fieldsDataSize = 0;
		byte[] fieldTypes = new byte[data.fields().size()];
		int fieldId = 0;
		for (Object field : data.fields()) {
			assert field != null;
			if (field instanceof Float) {
				fieldsDataSize += Float.BYTES;
				fieldTypes[fieldId] = FieldType.FLOAT.ordinalByte();
			} else if (field instanceof Double) {
				fieldsDataSize += Double.BYTES;
				fieldTypes[fieldId] = FieldType.DOUBLE.ordinalByte();
			} else if (field instanceof Integer) {
				fieldsDataSize += Integer.BYTES;
				fieldTypes[fieldId] = FieldType.INT.ordinalByte();
			} else if (field instanceof Long) {
				fieldsDataSize += Long.BYTES;
				fieldTypes[fieldId] = FieldType.LONG.ordinalByte();
			} else {
				throw new UnsupportedOperationException("Unsupported field type " + field.getClass());
			}
			fieldId++;
		}
		int size = Float.BYTES + Integer.BYTES + Integer.BYTES + Character.BYTES + (data.fields().size() + Byte.BYTES) + fieldsDataSize;
		var buf = allocator.apply(size);
		setScore(buf, data.score());
		setDoc(buf, data.doc());
		setShardIndex(buf, data.shardIndex());
		setFieldsCount(buf, data.fields().size());
		buf.writerIndex(size);

		fieldId = 0;
		for (Object field : data.fields()) {
			assert field != null;
			buf.writeByte(fieldTypes[fieldId]);
			if (field instanceof Float val) {
				buf.writeFloat(val);
			} else if (field instanceof Double val) {
				buf.writeDouble(val);
			} else if (field instanceof Integer val) {
				buf.writeInt(val);
			} else if (field instanceof Long val) {
				buf.writeLong(val);
			} else {
				throw new UnsupportedOperationException("Unsupported field type " + field.getClass());
			}
			fieldId++;
		}
		assert buf.writableBytes() == 0;
		return buf.asReadOnly();
	}

	@Override
	public LLFieldDoc deserialize(ByteBuf buf) {
		var fieldsCount = getFieldsCount(buf);
		ArrayList<Object> fields = new ArrayList<>(fieldsCount);
		buf.readerIndex(Float.BYTES + Integer.BYTES + Integer.BYTES + Character.BYTES);
		for (char i = 0; i < fieldsCount; i++) {
			fields.add(switch (FieldType.values()[buf.readByte()]) {
				case FLOAT -> buf.readFloat();
				case DOUBLE -> buf.readDouble();
				case INT -> buf.readInt();
				case LONG -> buf.readLong();
			});
		}
		assert buf.readableBytes() == 0;
		return new LLFieldDoc(getDoc(buf), getScore(buf), getShardIndex(buf), fields);
	}

	@Override
	public int compare(LLFieldDoc hitA, LLFieldDoc hitB) {
		if (hitA.score() == hitB.score()) {
			if (hitA.doc() == hitB.doc()) {
				return Integer.compare(hitA.shardIndex(), hitB.shardIndex());
			} else {
				return Integer.compare(hitB.doc(), hitA.doc());
			}
		} else {
			return Float.compare(hitA.score(), hitB.score());
		}
	}

	@Override
	public int compareDirect(ByteBuf hitA, ByteBuf hitB) {
		var scoreA = getScore(hitA);
		var scoreB = getScore(hitB);
		if (scoreA == scoreB) {
			var docA = getDoc(hitA);
			var docB = getDoc(hitB);
			if (docA == docB) {
				return Integer.compare(getShardIndex(hitA), getShardIndex(hitB));
			} else {
				return Integer.compare(docB, docA);
			}
		} else {
			return Float.compare(scoreA, scoreB);
		}
	}

	private static float getScore(ByteBuf hit) {
		return hit.getFloat(0);
	}

	private static int getDoc(ByteBuf hit) {
		return hit.getInt(Float.BYTES);
	}

	private static int getShardIndex(ByteBuf hit) {
		return hit.getInt(Float.BYTES + Integer.BYTES);
	}

	private char getFieldsCount(ByteBuf hit) {
		return hit.getChar(Float.BYTES + Integer.BYTES + Integer.BYTES);
	}

	private static void setScore(ByteBuf hit, float score) {
		hit.setFloat(0, score);
	}

	private static void setDoc(ByteBuf hit, int doc) {
		hit.setInt(Float.BYTES, doc);
	}

	private static void setShardIndex(ByteBuf hit, int shardIndex) {
		hit.setInt(Float.BYTES + Integer.BYTES, shardIndex);
	}

	private void setFieldsCount(ByteBuf hit, int size) {
		hit.setChar(Float.BYTES + Integer.BYTES + Integer.BYTES, (char) size);
	}
}
