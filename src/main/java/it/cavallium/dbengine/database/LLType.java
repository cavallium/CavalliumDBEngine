package it.cavallium.dbengine.database;

/**
 * <a href="https://lucene.apache.org/core/8_0_0/core/org/apache/lucene/document/Field.html">Field.html</a>
 */
public enum LLType {
	StringField,
	StringFieldStored,
	IntPoint,
	LongPoint,
	FloatPoint,
	DoublePoint,
	IntPointND,
	LongPointND,
	FloatPointND,
	DoublePointND,
	LongStoredField,
	BytesStoredField,
	NumericDocValuesField,
	SortedNumericDocValuesField,
	TextField,
	TextFieldStored
}
