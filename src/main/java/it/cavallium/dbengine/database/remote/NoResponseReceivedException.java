package it.cavallium.dbengine.database.remote;

public class NoResponseReceivedException extends IllegalStateException {

	public NoResponseReceivedException() {
		super("No response received");
	}

	public NoResponseReceivedException(Throwable cause) {
		super("No response received", cause);
	}
}
