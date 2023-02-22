package it.cavallium.dbengine.tests;

public class TestMemoryLLDictionaryLeaks extends TestLLDictionaryLeaks {

	private static final TemporaryDbGenerator GENERATOR = new MemoryTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
