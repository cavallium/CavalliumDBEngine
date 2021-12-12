package org.rocksdb;

public class KeyMayExistWorkaround {

	/**
	 * @return real value length
	 */
	public static int getValueLength(KeyMayExist keyMayExist) {
		return keyMayExist.valueLength;
	}

	/**
	 * 0 = not exists
	 *
	 * 1 = exists without value
	 *
	 * 2 = exists with value
	 *
	 */
	public static int getExistenceState(KeyMayExist keyMayExist) {
		return keyMayExist.exists.ordinal();
	}
}
