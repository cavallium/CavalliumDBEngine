package it.cavallium.dbengine.tests;

import it.cavallium.dbengine.tests.DbTestUtils.TempDb;
import java.io.IOException;

public interface TemporaryDbGenerator {

	TempDb openTempDb() throws IOException;

	void closeTempDb(TempDb db) throws IOException;
}
