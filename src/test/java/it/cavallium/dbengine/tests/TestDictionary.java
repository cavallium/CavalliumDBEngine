package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.tests.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.tests.DbTestUtils.isCIMode;
import static it.cavallium.dbengine.tests.DbTestUtils.tempDb;
import static it.cavallium.dbengine.tests.DbTestUtils.tempDictionary;

import it.cavallium.dbengine.database.UpdateMode;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class TestDictionary {

	protected abstract TemporaryDbGenerator getTempDbGenerator();

	private static Stream<Arguments> provideArgumentsCreate() {
		return Arrays.stream(UpdateMode.values()).map(Arguments::of);
	}

	@BeforeEach
	public void beforeEach() {
		ensureNoLeaks(false, false);
	}

	@AfterEach
	public void afterEach() {
		if (!isCIMode()) {
			ensureNoLeaks(true, false);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsCreate")
	public void testCreate(UpdateMode updateMode) throws IOException {
		tempDb(getTempDbGenerator(), db -> {
			tempDictionary(db, updateMode).clear();
			return null;
		});
	}
}
