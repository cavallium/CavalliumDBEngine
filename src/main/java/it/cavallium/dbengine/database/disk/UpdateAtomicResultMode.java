package it.cavallium.dbengine.database.disk;

public enum UpdateAtomicResultMode {
	NOTHING,
	PREVIOUS,
	CURRENT, BINARY_CHANGED,
	DELTA
}
