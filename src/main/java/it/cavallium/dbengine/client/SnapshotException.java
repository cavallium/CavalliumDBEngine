package it.cavallium.dbengine.client;

public class SnapshotException extends IllegalStateException {

	public SnapshotException(Throwable ex) {
		super(ex);
	}

	public SnapshotException(String message) {
		super(message);
	}
}
