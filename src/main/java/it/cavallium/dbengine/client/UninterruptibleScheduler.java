package it.cavallium.dbengine.client;

import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

public class UninterruptibleScheduler {

	public static Scheduler uninterruptibleScheduler(Scheduler scheduler) {
		return new Scheduler() {
			@Override
			public @NotNull Disposable schedule(@NotNull Runnable task) {
				scheduler.schedule(task);
				return () -> {};
			}

			@Override
			public @NotNull Disposable schedule(@NotNull Runnable task, long delay, @NotNull TimeUnit unit) {
				scheduler.schedule(task, delay, unit);
				return () -> {};
			}

			@Override
			public @NotNull Disposable schedulePeriodically(@NotNull Runnable task,
					long initialDelay,
					long period,
					@NotNull TimeUnit unit) {
				scheduler.schedulePeriodically(task, initialDelay, period, unit);
				return () -> {};
			}

			@Override
			public boolean isDisposed() {
				return scheduler.isDisposed();
			}

			@Override
			public void dispose() {
				scheduler.dispose();
			}

			@Override
			public void start() {
				scheduler.start();
			}

			@Override
			public long now(@NotNull TimeUnit unit) {
				return Scheduler.super.now(unit);
			}

			@Override
			public @NotNull Worker createWorker() {
				var worker = scheduler.createWorker();
				return new Worker() {
					@Override
					public @NotNull Disposable schedule(@NotNull Runnable task) {
						worker.schedule(task);
						return () -> {};
					}

					@Override
					public void dispose() {

					}

					@Override
					public boolean isDisposed() {
						return worker.isDisposed();
					}

					@Override
					public @NotNull Disposable schedule(@NotNull Runnable task, long delay, @NotNull TimeUnit unit) {
						worker.schedule(task, delay, unit);
						return () -> {};
					}

					@Override
					public @NotNull Disposable schedulePeriodically(@NotNull Runnable task,
							long initialDelay,
							long period,
							@NotNull TimeUnit unit) {
						worker.schedulePeriodically(task, initialDelay, period, unit);
						return () -> {};
					}
				};
			}
		};
	}
}
