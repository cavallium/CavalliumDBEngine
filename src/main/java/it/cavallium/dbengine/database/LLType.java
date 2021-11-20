package it.cavallium.dbengine.database;

/**
 * https://lucene.apache.org/core/8_0_0/core/org/apache/lucene/document/Field.html
 */
public enum LLType {
	StringField,
	StringFieldStored,
	IntPoint,
	LongPoint,
	LongStoredField,
	FloatPoint,
	NumericDocValuesField,
	SortedNumericDocValuesField,
	TextField,
	TextFieldStored
}
