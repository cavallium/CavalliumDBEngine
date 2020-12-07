package it.cavallium.dbengine.database;

public enum LLType {
	StringField,
	StringFieldStored,
	IntPoint,
	LongPoint,
	FloatPoint,
	SortedNumericDocValuesField,
	TextField,
	TextFieldStored
}
