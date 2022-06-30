package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.SafeCloseable;

public sealed interface UpdateAtomicResult extends DiscardingCloseable permits UpdateAtomicResultBinaryChanged,
		UpdateAtomicResultDelta, UpdateAtomicResultNothing, UpdateAtomicResultPrevious, UpdateAtomicResultCurrent {}
