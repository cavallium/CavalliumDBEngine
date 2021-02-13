package it.cavallium.dbengine.database;

public enum UpdateMode {
	/**
	 * Disallow update(). This speeds up the database reads and writes (x4 single writes, x1 multi writes)
	 */
	DISALLOW,
	/**
	 * Allow update(). This will slow down the database reads and writes (x1 single writes, x1 multi writes)
	 */
	ALLOW,
	/**
	 * Allow update(). This is as fast as {@link UpdateMode#DISALLOW} (x4 single writes, x1 multi writes),
	 * but you need to lock by yourself each key, otherwise the data will not be atomic!
	 */
	ALLOW_UNSAFE
}
