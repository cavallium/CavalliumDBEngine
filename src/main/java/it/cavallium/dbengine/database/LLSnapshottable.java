package it.cavallium.dbengine.database;

import java.io.IOException;

public interface LLSnapshottable {

	LLSnapshot takeSnapshot();

	void releaseSnapshot(LLSnapshot snapshot);
}
