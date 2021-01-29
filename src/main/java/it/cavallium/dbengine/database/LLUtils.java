package it.cavallium.dbengine.database;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import it.cavallium.dbengine.database.utils.RandomSortField;
import it.cavallium.dbengine.proto.LLKeyScore;
import it.cavallium.dbengine.proto.LLType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
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
		}
		return null;
	}

	public static ScoreMode toScoreMode(LLScoreMode scoreMode) {
		switch (scoreMode) {
			case COMPLETE: return ScoreMode.COMPLETE;
			case TOP_SCORES: return ScoreMode.TOP_SCORES;
			case COMPLETE_NO_SCORES: return ScoreMode.COMPLETE_NO_SCORES;
			default: throw new IllegalStateException("Unexpected value: " + scoreMode);
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

	public static Iterable<? extends it.cavallium.dbengine.proto.LLItem> toGrpc(LLItem[] items) {
		var list = new ArrayList<it.cavallium.dbengine.proto.LLItem>(items.length);
		for (LLItem item : items) {
			list.add(LLUtils.toGrpc(item));
		}
		return list;
	}

	public static it.cavallium.dbengine.proto.LLItem toGrpc(LLItem item) {
		var builder = it.cavallium.dbengine.proto.LLItem.newBuilder()
				.setType(LLType.valueOf(item.getType().toString()))
				.setName(item.getName())
				.setData1(ByteString.copyFrom(item.getData()));
		if (item.getData2() != null) {
			builder.setData2(ByteString.copyFrom(item.getData2()));
		}
		return builder.build();
	}

	public static it.cavallium.dbengine.proto.LLDocument toGrpc(LLDocument doc) {
		var builder = it.cavallium.dbengine.proto.LLDocument.newBuilder()
				.addAllItems(toGrpc(doc.getItems()));
		return builder.build();
	}

	public static Iterable<it.cavallium.dbengine.proto.LLDocument> toGrpc(
			Iterable<LLDocument> document) {
		LinkedList<it.cavallium.dbengine.proto.LLDocument> docs = new LinkedList<>();
		document.forEach((doc) -> docs.add(toGrpc(doc)));
		return docs;
	}

	public static Iterable<it.cavallium.dbengine.proto.LLTerm> toGrpcKey(Iterable<LLTerm> term) {
		LinkedList<it.cavallium.dbengine.proto.LLTerm> terms = new LinkedList<>();
		term.forEach((t) -> terms.add(toGrpc(t)));
		return terms;
	}

	public static it.cavallium.dbengine.proto.LLTerm toGrpc(LLTerm term) {
		return it.cavallium.dbengine.proto.LLTerm.newBuilder()
				.setKey(term.getKey())
				.setValue(term.getValue())
				.build();
	}

	public static it.cavallium.dbengine.proto.LLSort toGrpc(LLSort sort) {
		return it.cavallium.dbengine.proto.LLSort.newBuilder()
				.setFieldName(sort.getFieldName())
				.setType(it.cavallium.dbengine.proto.LLSortType.valueOf(sort.getType().toString()))
				.setReverse(sort.isReverse())
				.build();
	}

	public static it.cavallium.dbengine.database.LLKeyScore toKeyScore(LLKeyScore hit) {
		return new it.cavallium.dbengine.database.LLKeyScore(hit.getKey(), hit.getScore());
	}

	public static LLDocument toLocal(List<it.cavallium.dbengine.proto.LLItem> documentItemsList) {
		return new LLDocument(documentItemsList.stream().map(LLUtils::toLocal).toArray(LLItem[]::new));
	}

	public static LLDocument toLocal(it.cavallium.dbengine.proto.LLDocument document) {
		return toLocal(document.getItemsList());
	}

	public static List<LLDocument> toLocalDocuments(
			List<it.cavallium.dbengine.proto.LLDocument> documentItemsList) {
		return documentItemsList.stream().map(LLUtils::toLocal).collect(Collectors.toList());
	}

	public static List<LLTerm> toLocalTerms(List<it.cavallium.dbengine.proto.LLTerm> termItemsList) {
		return termItemsList.stream().map(LLUtils::toLocal).collect(Collectors.toList());
	}

	@SuppressWarnings("ConstantConditions")
	private static LLItem toLocal(it.cavallium.dbengine.proto.LLItem item) {
		var data2 = item.getData2() != null ? item.getData2().toByteArray() : null;
		return new LLItem(it.cavallium.dbengine.database.LLType.valueOf(item.getType().toString()),
				item.getName(), item.getData1().toByteArray(), data2);
	}

	public static LLTerm toLocal(it.cavallium.dbengine.proto.LLTerm key) {
		return new LLTerm(key.getKey(), key.getValue());
	}

	public static LLSort toLocal(it.cavallium.dbengine.proto.LLSort sort) {
		return new LLSort(sort.getFieldName(), LLSortType.valueOf(sort.getType().toString()),
				sort.getReverse());
	}

	public static LLKeyScore toGrpc(it.cavallium.dbengine.database.LLKeyScore hit) {
		return LLKeyScore.newBuilder()
				.setKey(hit.getKey())
				.setScore(hit.getScore())
				.build();
	}
}
