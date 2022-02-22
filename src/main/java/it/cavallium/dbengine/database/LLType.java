package it.cavallium.dbengine.database;

/**
 * https://lucene.apache.org/core/8_0_0/core/org/apache/lucene/document/Field.html
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
	NumericDocValuesField,
	SortedNumericDocValuesField,
	TextField,
	TextFieldStored
}
