package it.cavallium.dbengine.database.disk;

/** Based on:
 * https://gist.github.com/glandais-sparklane/e38834aa9df0c56f23e2d8d2e6899c78
 */

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@SuppressWarnings("ReactiveStreamsPublisherImplementation")
class BufferTimeOutPublisher<T> implements Publisher<List<T>> {

	private static final ScheduledExecutorService EXECUTOR = Executors.newSingleThreadScheduledExecutor();

	private final Publisher<T> source;
	private final int size;
	private final long duration;

	public BufferTimeOutPublisher(Publisher<T> source, int size, Duration duration) {
		this.source = source;
		this.size = size;
		this.duration = duration.toMillis();
	}

	@Override
	public void subscribe(Subscriber<? super List<T>> subscriber) {
		subscriber.onSubscribe(new BufferTimeOutSubscription<T>(source, subscriber, size, duration));
	}

	protected static class BufferTimeOutSubscription<T> implements Subscription, Subscriber<T> {

		private final Subscriber<? super List<T>> subscriber;
		private final int size;
		private final long duration;
		private Subscription subscription;

		private final ReentrantLock lock = new ReentrantLock();

		private List<T> buffer;
		private ScheduledFuture<?> scheduledFuture;

		private long downstreamRequests = 0;
		private long downstreamTransmit = 0;

		private long upstreamRequests = 0;
		private long upstreamTransmit = 0;
		private boolean upstreamCompleted = false;

		public BufferTimeOutSubscription(Publisher<T> source,
				Subscriber<? super List<T>> subscriber,
				int size,
				long duration) {
			this.subscriber = subscriber;
			this.size = size;
			this.duration = duration;
			this.buffer = new ArrayList<>(size);
			source.subscribe(this);
		}

		// downstream
		@Override
		public void request(long n) {
			lock.lock();
			downstreamRequests = downstreamRequests + n;

			checkSend();

			long downstreamMax = (downstreamRequests - downstreamTransmit) * size;
			long upstreamRequested = upstreamRequests - upstreamTransmit;
			long toRequest = downstreamMax - upstreamRequested;

			if (toRequest > 0) {
				subscription.request(toRequest);
				upstreamRequests = upstreamRequests + toRequest;
			}
			lock.unlock();
		}

		@Override
		public void cancel() {
			subscription.cancel();
		}

		// upstream
		@Override
		public void onSubscribe(Subscription s) {
			this.subscription = s;
			scheduledFuture = EXECUTOR.scheduleAtFixedRate(this::timeout, 0, this.duration, TimeUnit.MILLISECONDS);
		}

		private void timeout() {
			checkSend();
		}

		private void checkSend() {
			lock.lock();
			if (!this.buffer.isEmpty() && downstreamRequests > downstreamTransmit) {
				List<T> output = prepareOutput();
				subscriber.onNext(output);
				downstreamTransmit++;
				if (!this.buffer.isEmpty()) {
					checkSend();
				}
			}
			if (upstreamCompleted && downstreamRequests > downstreamTransmit) {
				scheduledFuture.cancel(false);
				subscriber.onComplete();
			}
			lock.unlock();
		}

		private List<T> prepareOutput() {
			if (this.buffer.size() > size) {
				List<T> output = new ArrayList<>(this.buffer.subList(0, size));
				this.buffer = new ArrayList<>(this.buffer.subList(size, this.buffer.size()));
				return output;
			} else {
				List<T> output = this.buffer;
				this.buffer = new ArrayList<>(size);
				return output;
			}
		}

		@Override
		public void onNext(T t) {
			lock.lock();
			this.buffer.add(t);
			upstreamTransmit++;
			if (this.buffer.size() == size) {
				checkSend();
			}
			lock.unlock();
		}

		@Override
		public void onError(Throwable t) {
			scheduledFuture.cancel(false);
			subscriber.onError(t);
		}

		@Override
		public void onComplete() {
			lock.lock();
			upstreamCompleted = true;
			checkSend();
			lock.unlock();
		}

	}
}
