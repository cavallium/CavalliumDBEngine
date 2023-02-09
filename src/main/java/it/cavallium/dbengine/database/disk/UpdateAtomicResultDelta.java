package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLDelta;

public record UpdateAtomicResultDelta(LLDelta delta) implements UpdateAtomicResult {
}
