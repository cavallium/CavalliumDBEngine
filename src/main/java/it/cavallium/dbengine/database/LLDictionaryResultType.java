package it.cavallium.dbengine.database;

import org.jetbrains.annotations.Nullable;

public enum LLDictionaryResultType {
	VOID, VALUE_CHANGED, PREVIOUS_VALUE;

	public static LLDictionaryResultType valueOf(@Nullable it.cavallium.dbengine.proto.LLDictionaryResultType resultType) {
		if (resultType == null || resultType == it.cavallium.dbengine.proto.LLDictionaryResultType.UNRECOGNIZED) {
			return VOID;
		}

		switch (resultType) {
			case PREVIOUS_VALUE:
				return PREVIOUS_VALUE;
			case VALUE_CHANGED:
				return VALUE_CHANGED;
			case VOID:
				return VOID;
		}
		return VOID;
	}

	public it.cavallium.dbengine.proto.LLDictionaryResultType toProto() {
		switch (this) {
			case VALUE_CHANGED:
				return it.cavallium.dbengine.proto.LLDictionaryResultType.VALUE_CHANGED;
			case PREVIOUS_VALUE:
				return it.cavallium.dbengine.proto.LLDictionaryResultType.PREVIOUS_VALUE;
			case VOID:
				return it.cavallium.dbengine.proto.LLDictionaryResultType.VOID;
		}

		return it.cavallium.dbengine.proto.LLDictionaryResultType.UNRECOGNIZED;
	}
}
