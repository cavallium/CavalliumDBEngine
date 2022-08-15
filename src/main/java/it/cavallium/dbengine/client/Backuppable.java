package it.cavallium.dbengine.client;

import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

public abstract class Backuppable implements IBackuppable {

	public enum State {
		RUNNING, PAUSING, PAUSED, RESUMING, STOPPED
	}

	private final AtomicInteger state = new AtomicInteger();

	@Override
	public final Mono<Void> pauseForBackup() {
		return Mono.defer(() -> {
			if (state.compareAndSet(State.RUNNING.ordinal(), State.PAUSING.ordinal())) {
				return onPauseForBackup().doFinally(type -> state.compareAndSet(State.PAUSING.ordinal(),
						type == SignalType.ON_ERROR ? State.RUNNING.ordinal() : State.PAUSED.ordinal()
				));
			} else {
				return Mono.empty();
			}
		});
	}

	@Override
	public final Mono<Void> resumeAfterBackup() {
		return Mono.defer(() -> {
			if (state.compareAndSet(State.PAUSED.ordinal(), State.RESUMING.ordinal())) {
				return onResumeAfterBackup().doFinally(type -> state.compareAndSet(State.RESUMING.ordinal(),
						type == SignalType.ON_ERROR ? State.PAUSED.ordinal() : State.RUNNING.ordinal()
				));
			} else {
				return Mono.empty();
			}
		});
	}

	@Override
	public final boolean isPaused() {
		return state.get() == State.PAUSED.ordinal();
	}

	public final State getState() {
		return State.values()[state.get()];
	}

	protected abstract Mono<Void> onPauseForBackup();

	protected abstract Mono<Void> onResumeAfterBackup();

	public final void setStopped() {
		state.set(State.STOPPED.ordinal());
	}
}
