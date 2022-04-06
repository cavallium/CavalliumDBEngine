package it.cavallium.dbengine.lucene;

import io.netty5.buffer.api.Buffer;
import java.util.ArrayList;
import java.util.function.Function;

public class LLFieldDocCodec implements HugePqCodec<LLFieldDoc> {

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
	public Buffer serialize(Function<Integer, Buffer> allocator, LLFieldDoc data) {
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
		buf.writerOffset(size);

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
		return buf;
	}

	@Override
	public LLFieldDoc deserialize(Buffer buf) {
		var fieldsCount = getFieldsCount(buf);
		ArrayList<Object> fields = new ArrayList<>(fieldsCount);
		buf.readerOffset(Float.BYTES + Integer.BYTES + Integer.BYTES + Character.BYTES);
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

	private static float getScore(Buffer hit) {
		return HugePqCodec.getLexFloat(hit, 0, false);
	}

	private static int getDoc(Buffer hit) {
		return HugePqCodec.getLexInt(hit, Float.BYTES, true);
	}

	private static int getShardIndex(Buffer hit) {
		return HugePqCodec.getLexInt(hit, Float.BYTES + Integer.BYTES, false);
	}

	private char getFieldsCount(Buffer hit) {
		return hit.getChar(Float.BYTES + Integer.BYTES + Integer.BYTES);
	}

	private static void setScore(Buffer hit, float score) {
		HugePqCodec.setLexFloat(hit, 0, false, score);
	}

	private static void setDoc(Buffer hit, int doc) {
		HugePqCodec.setLexInt(hit, Float.BYTES, true, doc);
	}

	private static void setShardIndex(Buffer hit, int shardIndex) {
		HugePqCodec.setLexInt(hit, Float.BYTES + Integer.BYTES, false, shardIndex);
	}

	private void setFieldsCount(Buffer hit, int size) {
		hit.setChar(Float.BYTES + Integer.BYTES + Integer.BYTES, (char) size);
	}

	@Override
	public LLFieldDoc clone(LLFieldDoc obj) {
		return new LLFieldDoc(obj.doc(), obj.score(), obj.shardIndex(), obj.fields());
	}
}
