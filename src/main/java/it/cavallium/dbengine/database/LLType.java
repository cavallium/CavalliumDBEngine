package it.cavallium.dbengine.database;

public enum LLType {
	StringField,
	StringFieldStored,
	IntPoint,
	LongPoint,
	LongStoredField,
	FloatPoint,
	SortedNumericDocValuesField,
	TextField,
	TextFieldStored
}
