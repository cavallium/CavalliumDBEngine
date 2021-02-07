package it.cavallium.dbengine.client;

public class SnapshotException extends RuntimeException {

	public SnapshotException(Throwable ex) {
		super(ex);
	}

	public SnapshotException(String message) {
		super(message);
	}
}
