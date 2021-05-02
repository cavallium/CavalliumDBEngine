package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.tempDb;
import static it.cavallium.dbengine.DbTestUtils.tempDictionary;

import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.UpdateMode;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.test.StepVerifier;

public class TestDictionary {

	private static Stream<Arguments> provideArgumentsCreate() {
		return Arrays.stream(UpdateMode.values()).map(Arguments::of);
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsCreate")
	public void testCreate(UpdateMode updateMode) {
		StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.flatMap(LLDictionary::clear)
						.then()
				))
				.verifyComplete();
	}
}
