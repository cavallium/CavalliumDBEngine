package it.cavallium.dbengine.database;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.lucene.document.Field;

public class LLItem {

	private final LLType type;
	private final String name;
	private final byte[] data;
	// nullable
	private final byte[] data2;

	public LLItem(LLType type, String name, byte[] data, byte[] data2) {
		this.type = type;
		this.name = name;
		this.data = data;
		this.data2 = data2;
	}

	private LLItem(LLType type, String name, String data) {
		this.type = type;
		this.name = name;
		this.data = data.getBytes(StandardCharsets.UTF_8);
		this.data2 = null;
	}

	private LLItem(LLType type, String name, int data) {
		this.type = type;
		this.name = name;
		this.data = Ints.toByteArray(data);
		this.data2 = null;
	}

	private LLItem(LLType type, String name, float data) {
		this.type = type;
		this.name = name;
		this.data = ByteBuffer.allocate(4).putFloat(data).array();;
		this.data2 = null;
	}

	private LLItem(LLType type, String name, long data) {
		this.type = type;
		this.name = name;
		this.data = Longs.toByteArray(data);
		this.data2 = null;
	}

	public static LLItem newIntPoint(String name, int data) {
		return new LLItem(LLType.IntPoint, name, data);
	}

	public static LLItem newLongPoint(String name, long data) {
		return new LLItem(LLType.LongPoint, name, data);
	}

	public static LLItem newFloatPoint(String name, float data) {
		return new LLItem(LLType.FloatPoint, name, data);
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

	public static LLItem newSortedNumericDocValuesField(String name, long data) {
		return new LLItem(LLType.SortedNumericDocValuesField, name, data);
	}

	public String getName() {
		return name;
	}

	public LLType getType() {
		return type;
	}

	public byte[] getData() {
		return data;
	}

	public byte[] getData2() {
		return data2;
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
		return type == llItem.type &&
				Objects.equals(name, llItem.name) &&
				Arrays.equals(data, llItem.data) &&
				Arrays.equals(data2, llItem.data2);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(type, name);
		result = 31 * result + Arrays.hashCode(data);
		result = 31 * result + Arrays.hashCode(data2);
		return result;
	}

	@Override
	public String toString() {
		var sj = new StringJoiner(", ", "[", "]")
				.add("type=" + type)
				.add("name='" + name + "'");
		if (data != null && data.length > 0) {
				sj.add("data=" + new String(data));
		}
		if (data2 != null && data2.length > 0) {
				sj.add("data2=" + new String(data2));
		}
				return sj.toString();
	}

	public String stringValue() {
		return new String(data, StandardCharsets.UTF_8);
	}
}
