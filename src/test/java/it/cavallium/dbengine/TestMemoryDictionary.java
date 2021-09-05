package it.cavallium.dbengine;

public class TestMemoryDictionary extends TestDictionary {

	private static final TemporaryDbGenerator GENERATOR = new MemoryTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}