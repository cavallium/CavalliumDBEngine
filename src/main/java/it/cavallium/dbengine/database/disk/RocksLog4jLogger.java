package it.cavallium.dbengine.database.disk;

import org.apache.logging.log4j.Logger;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;

class RocksLog4jLogger extends org.rocksdb.Logger {

	private final Logger logger;

	public RocksLog4jLogger(DBOptions rocksdbOptions, Logger logger) {
		super(rocksdbOptions);
		this.logger = logger;
	}

	@Override
	protected void log(InfoLogLevel infoLogLevel, String logMsg) {
		switch (infoLogLevel) {
			case DEBUG_LEVEL -> logger.debug(logMsg);
			case INFO_LEVEL -> logger.info(logMsg);
			case WARN_LEVEL -> logger.warn(logMsg);
			case ERROR_LEVEL -> logger.error(logMsg);
			case FATAL_LEVEL -> logger.fatal(logMsg);
			case HEADER_LEVEL -> logger.trace(logMsg);
			default -> throw new UnsupportedOperationException(infoLogLevel + " level not supported");
		}
	}
}
