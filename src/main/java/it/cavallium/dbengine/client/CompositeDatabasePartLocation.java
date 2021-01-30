package it.cavallium.dbengine.client;

import java.util.Objects;
import java.util.StringJoiner;

public class CompositeDatabasePartLocation {
	private final CompositeDatabasePartType partType;
	private final String partName;

	private CompositeDatabasePartLocation(CompositeDatabasePartType partType, String partName) {
		this.partType = partType;
		this.partName = partName;
	}

	public static CompositeDatabasePartLocation of(CompositeDatabasePartType partType, String partName) {
		return new CompositeDatabasePartLocation(partType, partName);
	}

	public enum CompositeDatabasePartType {
		KV_DATABASE,
		LUCENE_INDEX
	}

	public CompositeDatabasePartType getPartType() {
		return partType;
	}

	public String getPartName() {
		return partName;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CompositeDatabasePartLocation that = (CompositeDatabasePartLocation) o;
		return partType == that.partType && Objects.equals(partName, that.partName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(partType, partName);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", CompositeDatabasePartLocation.class.getSimpleName() + "[", "]")
				.add("partType=" + partType)
				.add("partName='" + partName + "'")
				.toString();
	}
}
