package it.cavallium.dbengine.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class UTFUtils {
	public static void writeUTF(DataOutput out, String utf) throws IOException {
		byte[] bytes = utf.getBytes(StandardCharsets.UTF_8);
		out.writeInt(bytes.length);
		out.write(bytes);
	}

	public static String readUTF(DataInput in) throws IOException {
		int len = in.readInt();
		byte[] data = new byte[len];
		in.readFully(data, 0, len);
		return new String(data, StandardCharsets.UTF_8);
	}

	/**
	 * Keep only ascii alphanumeric letters
	 */
	public static String keepOnlyASCII(String nextString) {
		char[] chars = nextString.toCharArray();
		//noinspection UnusedAssignment
		nextString = null;
		int writeIndex = 0;
		char c;
		for (int checkIndex = 0; checkIndex < chars.length; checkIndex++) {
			c = chars[checkIndex];
			if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
				if (writeIndex != checkIndex) {
					chars[writeIndex] = c;
				}
				writeIndex++;
			}
		}
		return new String(chars, 0, writeIndex);
	}
}
