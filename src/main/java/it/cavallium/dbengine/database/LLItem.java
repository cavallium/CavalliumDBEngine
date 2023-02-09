package it.cavallium.dbengine.database;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BytesRef;

public class LLItem {

	private final LLType type;
	private final String name;
	private final Object data;

	public LLItem(LLType type, String name, ByteBuffer data) {
		this.type = type;
		this.name = name;
		this.data = data;
	}

	public LLItem(LLType type, String name, BytesRef data) {
		this.type = type;
		this.name = name;
		this.data = data;
	}

	public LLItem(LLType type, String name, KnnFieldData data) {
		this.type = type;
		this.name = name;
		this.data = data;
	}

	private LLItem(LLType type, String name, String data) {
		this.type = type;
		this.name = name;
		this.data = data;
	}

	private LLItem(LLType type, String name, int data) {
		this.type = type;
		this.name = name;
		this.data = data;
	}

	private LLItem(LLType type, String name, float data) {
		this.type = type;
		this.name = name;
		this.data = data;
	}

	private LLItem(LLType type, String name, long data) {
		this.type = type;
		this.name = name;
		this.data = data;
	}

	private LLItem(LLType type, String name, int... data) {
		this.type = type;
		this.name = name;
		this.data = data;
	}

	private LLItem(LLType type, String name, float... data) {
		this.type = type;
		this.name = name;
		this.data = data;
	}

	private LLItem(LLType type, String name, double... data) {
		this.type = type;
		this.name = name;
		this.data = data;
	}

	private LLItem(LLType type, String name, long... data) {
		this.type = type;
		this.name = name;
		this.data = data;
	}

	public static LLItem newIntPoint(String name, int data) {
		return new LLItem(LLType.IntPoint, name, data);
	}

	public static LLItem newIntPointND(String name, int... data) {
		return new LLItem(LLType.IntPointND, name, data);
	}

	public static LLItem newLongPoint(String name, long data) {
		return new LLItem(LLType.LongPoint, name, data);
	}

	public static LLItem newFloatPoint(String name, float data) {
		return new LLItem(LLType.FloatPoint, name, data);
	}

	public static LLItem newDoublePoint(String name, double data) {
		return new LLItem(LLType.DoublePoint, name, data);
	}

	public static LLItem newLongPointND(String name, long... data) {
		return new LLItem(LLType.LongPointND, name, data);
	}

	public static LLItem newFloatPointND(String name, float... data) {
		return new LLItem(LLType.FloatPointND, name, data);
	}

	public static LLItem newDoublePointND(String name, double... data) {
		return new LLItem(LLType.DoublePointND, name, data);
	}

	public static LLItem newLongStoredField(String name, long data) {
		return new LLItem(LLType.LongStoredField, name, data);
	}

	public static LLItem newLongStoredFieldND(String name, long... data) {
		BytesRef packed = LongPoint.pack(data);
		return new LLItem(LLType.BytesStoredField, name, packed);
	}

	public static LLItem newTextField(String name, String data, Field.Store store) {
		if (store == Field.Store.YES) {
			return new LLItem(LLType.TextFieldStored, name, data);
		} else {
			return new LLItem(LLType.TextField, name, data);
		}
	}

	public static LLItem newStringField(String name, String data, Field.Store store) {
		if (store == Field.Store.YES) {
			return new LLItem(LLType.StringFieldStored, name, data);
		} else {
			return new LLItem(LLType.StringField, name, data);
		}
	}

	public static LLItem newStringField(String name, BytesRef bytesRef, Field.Store store) {
		if (store == Field.Store.YES) {
			return new LLItem(LLType.StringFieldStored, name, bytesRef);
		} else {
			return new LLItem(LLType.StringField, name, bytesRef);
		}
	}

	public static LLItem newSortedNumericDocValuesField(String name, long data) {
		return new LLItem(LLType.SortedNumericDocValuesField, name, data);
	}

	public static LLItem newNumericDocValuesField(String name, long data) {
		return new LLItem(LLType.NumericDocValuesField, name, data);
	}

	public static LLItem newKnnField(String name, KnnFieldData knnFieldData) {
		return new LLItem(LLType.NumericDocValuesField, name, knnFieldData);
	}

	public String getName() {
		return name;
	}

	public LLType getType() {
		return type;
	}

	public Object getData() {
		return data;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		LLItem llItem = (LLItem) o;

		if (type != llItem.type) {
			return false;
		}
		return Objects.equals(name, llItem.name);
	}

	@Override
	public int hashCode() {
		int result = type != null ? type.hashCode() : 0;
		result = 31 * result + (name != null ? name.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLItem.class.getSimpleName() + "[", "]")
				.add("type=" + type)
				.add("name='" + name + "'")
				.add("data=" + data)
				.toString();
	}

	public int intData() {
		return (int) data;
	}

	public int[] intArrayData() {
		return (int[]) data;
	}

	public long longData() {
		return (long) data;
	}

	public long[] longArrayData() {
		return (long[]) data;
	}

	public float floatData() {
		return (float) data;
	}

	public float[] floatArrayData() {
		return (float[]) data;
	}

	public double doubleData() {
		return (double) data;
	}

	public double[] doubleArrayData() {
		return (double[]) data;
	}

	public KnnFieldData knnFieldData() {
		return (KnnFieldData) data;
	}

	public String stringValue() {
		return (String) data;
	}

	public record KnnFieldData(float[] data, VectorSimilarityFunction vectorSimilarityFunction) {}
}
