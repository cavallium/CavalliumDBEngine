package it.cavallium.dbengine.database.disk;

public sealed interface UpdateAtomicResult permits UpdateAtomicResultBinaryChanged,
		UpdateAtomicResultDelta, UpdateAtomicResultNothing, UpdateAtomicResultPrevious, UpdateAtomicResultCurrent {}
