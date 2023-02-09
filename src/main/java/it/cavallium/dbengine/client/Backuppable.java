package it.cavallium.dbengine.client;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Backuppable implements IBackuppable {

	public enum State {
		RUNNING, PAUSING, PAUSED, RESUMING, STOPPED
	}

	private final AtomicInteger state = new AtomicInteger();

	@Override
	public final void pauseForBackup() {
		if (state.compareAndSet(State.RUNNING.ordinal(), State.PAUSING.ordinal())) {
			try {
				onPauseForBackup();
				state.compareAndSet(State.PAUSING.ordinal(), State.PAUSED.ordinal());
			} catch (Throwable ex) {
				state.compareAndSet(State.PAUSING.ordinal(), State.RUNNING.ordinal());
				throw ex;
			}
		}
	}

	@Override
	public final void resumeAfterBackup() {
		if (state.compareAndSet(State.PAUSED.ordinal(), State.RESUMING.ordinal())) {
			try {
				onResumeAfterBackup();
				state.compareAndSet(State.RESUMING.ordinal(), State.RUNNING.ordinal());
			} catch (Throwable ex) {
				state.compareAndSet(State.RESUMING.ordinal(), State.PAUSED.ordinal());
				throw ex;
			}
		}
	}

	@Override
	public final boolean isPaused() {
		return state.get() == State.PAUSED.ordinal();
	}

	public final State getState() {
		return State.values()[state.get()];
	}

	protected abstract void onPauseForBackup();

	protected abstract void onResumeAfterBackup();

	public final void setStopped() {
		state.set(State.STOPPED.ordinal());
	}
}
