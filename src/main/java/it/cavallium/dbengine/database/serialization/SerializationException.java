package it.cavallium.dbengine.database.serialization;

import java.io.IOException;

public class SerializationException extends IOException {

	public SerializationException() {
		super();
	}

	public SerializationException(String message) {
		super(message);
	}

	public SerializationException(String message, Throwable cause) {
		super(message, cause);
	}

	public SerializationException(Throwable cause) {
		super(cause);
	}

}