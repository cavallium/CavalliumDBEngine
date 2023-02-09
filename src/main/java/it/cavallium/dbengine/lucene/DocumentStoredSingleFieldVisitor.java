package it.cavallium.dbengine.lucene;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

public class DocumentStoredSingleFieldVisitor extends StoredFieldVisitor {
	private final Document doc = new Document();
	private final String fieldToAdd;

	public DocumentStoredSingleFieldVisitor(String fieldToAdd) {
		this.fieldToAdd = fieldToAdd;
	}

	public DocumentStoredSingleFieldVisitor() {
		this.fieldToAdd = null;
	}

	public void binaryField(FieldInfo fieldInfo, byte[] value) {
		this.doc.add(new StoredField(fieldInfo.name, value));
	}

	public void stringField(FieldInfo fieldInfo, String value) {
		FieldType ft = new FieldType(TextField.TYPE_STORED);
		ft.setStoreTermVectors(fieldInfo.hasVectors());
		ft.setOmitNorms(fieldInfo.omitsNorms());
		ft.setIndexOptions(fieldInfo.getIndexOptions());
		this.doc.add(new StoredField(fieldInfo.name, (String)Objects.requireNonNull(value, "String value should not be null"), ft));
	}

	public void intField(FieldInfo fieldInfo, int value) {
		this.doc.add(new StoredField(fieldInfo.name, value));
	}

	public void longField(FieldInfo fieldInfo, long value) {
		this.doc.add(new StoredField(fieldInfo.name, value));
	}

	public void floatField(FieldInfo fieldInfo, float value) {
		this.doc.add(new StoredField(fieldInfo.name, value));
	}

	public void doubleField(FieldInfo fieldInfo, double value) {
		this.doc.add(new StoredField(fieldInfo.name, value));
	}

	public Status needsField(FieldInfo fieldInfo) {
		return Objects.equals(this.fieldToAdd, fieldInfo.name) ? Status.YES : Status.NO;
	}

	public Document getDocument() {
		return this.doc;
	}
}
