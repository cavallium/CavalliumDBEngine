package it.cavallium.dbengine.client;

public interface IBackuppable {

	void pauseForBackup();

	void resumeAfterBackup();

	boolean isPaused();
}
