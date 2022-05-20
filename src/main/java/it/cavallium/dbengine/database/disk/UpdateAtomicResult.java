package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.SafeCloseable;

public sealed interface UpdateAtomicResult extends SafeCloseable permits UpdateAtomicResultBinaryChanged,
		UpdateAtomicResultDelta, UpdateAtomicResultNothing, UpdateAtomicResultPrevious, UpdateAtomicResultCurrent {}
