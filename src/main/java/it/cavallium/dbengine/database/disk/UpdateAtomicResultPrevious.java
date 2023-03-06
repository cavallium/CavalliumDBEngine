package it.cavallium.dbengine.database.disk;

import it.cavallium.buffer.Buf;

public record UpdateAtomicResultPrevious(Buf previous) implements UpdateAtomicResult {

}
