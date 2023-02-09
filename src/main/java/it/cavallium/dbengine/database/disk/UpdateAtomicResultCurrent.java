package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.buffers.Buf;

public record UpdateAtomicResultCurrent(Buf current) implements UpdateAtomicResult {
}
