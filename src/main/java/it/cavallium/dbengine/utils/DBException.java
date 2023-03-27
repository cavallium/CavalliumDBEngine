package it.cavallium.dbengine.utils;

import java.io.IOException;
import java.io.UncheckedIOException;

public class DBException extends IllegalStateException {

	public DBException(String message) {
		super(message);
	}

	public DBException(String message, Exception cause) {
		super(message, cause);
	}

	public DBException(Exception cause) {
		super(cause);
	}

	public DBException() {
		super();
	}
}
