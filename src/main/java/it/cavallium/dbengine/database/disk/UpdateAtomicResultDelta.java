package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLDelta;

public final record UpdateAtomicResultDelta(Send<LLDelta> delta) implements UpdateAtomicResult {}
