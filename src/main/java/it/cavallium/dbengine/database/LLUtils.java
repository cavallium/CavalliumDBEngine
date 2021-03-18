package it.cavallium.dbengine.database;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.cavallium.dbengine.lucene.RandomSortField;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
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
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public class LLUtils {

	private static final byte[] RESPONSE_TRUE = new byte[]{1};
	private static final byte[] RESPONSE_FALSE = new byte[]{0};
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

	public static byte[] booleanToResponse(boolean bool) {
		return bool ? RESPONSE_TRUE : RESPONSE_FALSE;
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
}
