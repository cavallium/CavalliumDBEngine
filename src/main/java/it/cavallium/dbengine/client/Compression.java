package it.cavallium.dbengine.client;

import org.rocksdb.CompressionType;

public enum Compression {
	PLAIN(CompressionType.NO_COMPRESSION),
	SNAPPY(CompressionType.SNAPPY_COMPRESSION),
	LZ4(CompressionType.LZ4_COMPRESSION),
	LZ4_HC(CompressionType.LZ4HC_COMPRESSION),
	ZSTD(CompressionType.ZSTD_COMPRESSION),
	ZLIB(CompressionType.ZLIB_COMPRESSION),
	BZLIB2(CompressionType.BZLIB2_COMPRESSION);

	private final CompressionType type;

	Compression(CompressionType compressionType) {
		this.type = compressionType;
	}

	public CompressionType getType() {
		return type;
	}
}
