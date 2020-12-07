package it.cavallium.dbengine.database;

import java.io.IOException;

public interface LLSnapshottable {

	LLSnapshot takeSnapshot() throws IOException;

	void releaseSnapshot(LLSnapshot snapshot) throws IOException;
}
