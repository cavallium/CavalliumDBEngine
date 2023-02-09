package it.cavallium.dbengine.database.collections;

import org.jetbrains.annotations.Nullable;

public interface ValueGetter<KEY, VALUE> {

	/**
	 * Can return Mono error IOException
	 */
	@Nullable VALUE get(KEY key);
}
