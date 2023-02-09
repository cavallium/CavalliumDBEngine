package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.buffers.Buf;

public record UpdateAtomicResultPrevious(Buf previous) implements UpdateAtomicResult {

}
