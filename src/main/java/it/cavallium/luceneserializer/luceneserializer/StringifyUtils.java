package it.cavallium.luceneserializer.luceneserializer;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.lucene.index.Term;

public class StringifyUtils {

	public static void stringifyFloat(StringBuilder output, float value) {
		writeHeader(output, QueryConstructorType.FLOAT, new StringBuilder().append(value));
	}

	public static void stringifyInt(StringBuilder output, int value) {
		writeHeader(output, QueryConstructorType.INT, new StringBuilder().append(value));
	}

	public static void stringifyLong(StringBuilder output, long value) {
		writeHeader(output, QueryConstructorType.LONG, new StringBuilder().append(value));
	}

	public static void stringifyBool(StringBuilder output, boolean value) {
		writeHeader(output, QueryConstructorType.BOOLEAN, new StringBuilder().append(value));
	}

	public static void stringifyString(StringBuilder output, String value) {
		writeHeader(output, QueryConstructorType.STRING, new StringBuilder()
				.append(Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8))));
	}

	public static void stringifyTerm(StringBuilder output, Term value) {
		var data = new StringBuilder();
		stringifyString(data, value.field());
		stringifyString(data, value.text());
		writeHeader(output, QueryConstructorType.TERM, data);
	}

	public static void stringifyTermPosition(StringBuilder output, TermPosition value) {
		var data = new StringBuilder();
		stringifyTerm(data, value.getTerm());
		stringifyInt(data, value.getPosition());
		writeHeader(output, QueryConstructorType.TERM_POSITION, data);
	}

	public static void stringifyNullTerm(StringBuilder output) {
		writeHeader(output, QueryConstructorType.NULL, new StringBuilder());
	}

	public static void writeHeader(StringBuilder output, QueryConstructorType type,
			StringBuilder data) {
		output.append(Integer.toHexString(data.length())).append('|');
		output.append(type.ordinal()).append('|');
		output.append(data);
	}
}
