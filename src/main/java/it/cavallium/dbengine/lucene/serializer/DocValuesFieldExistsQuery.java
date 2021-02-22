package it.cavallium.dbengine.lucene.serializer;

public class DocValuesFieldExistsQuery implements Query {

	private final String field;

	public DocValuesFieldExistsQuery(String field) {
		this.field = field;
	}

	public String getField() {
		return field;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		StringifyUtils.stringifyString(data, field);
		StringifyUtils.writeHeader(output, QueryConstructorType.DOC_VALUES_FIELD_EXISTS_QUERY, data);
	}

	@Override
	public String toString() {
		return "(existence of field " + field + ")";
	}
}
