package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.destroyAllocator;
import static it.cavallium.dbengine.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.DbTestUtils.newAllocator;
import static it.cavallium.dbengine.DbTestUtils.tempDb;
import static it.cavallium.dbengine.DbTestUtils.tempDictionary;

import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.UpdateMode;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.test.StepVerifier;

public abstract class TestDictionary {

	private TestAllocator allocator;

	protected abstract TemporaryDbGenerator getTempDbGenerator();

	private static Stream<Arguments> provideArgumentsCreate() {
		return Arrays.stream(UpdateMode.values()).map(Arguments::of);
	}

	@BeforeEach
	public void beforeEach() {
		this.allocator = newAllocator();
		ensureNoLeaks(allocator.allocator(), false);
	}

	@AfterEach
	public void afterEach() {
		ensureNoLeaks(allocator.allocator(), true);
		destroyAllocator(allocator);
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsCreate")
	public void testCreate(UpdateMode updateMode) {
		StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.flatMap(LLDictionary::clear)
						.then()
				))
				.verifyComplete();
	}
}
