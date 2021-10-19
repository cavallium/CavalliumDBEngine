package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Send;

public final record UpdateAtomicResultCurrent(Send<Buffer> current) implements UpdateAtomicResult {}
