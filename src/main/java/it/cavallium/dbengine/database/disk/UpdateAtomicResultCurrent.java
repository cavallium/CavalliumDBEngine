package it.cavallium.dbengine.database.disk;

import it.cavallium.buffer.Buf;

public record UpdateAtomicResultCurrent(Buf current) implements UpdateAtomicResult {
}
