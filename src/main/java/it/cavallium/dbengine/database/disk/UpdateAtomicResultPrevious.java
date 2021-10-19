package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Send;

public final record UpdateAtomicResultPrevious(Send<Buffer> previous) implements UpdateAtomicResult {}
