package it.cavallium.dbengine.client;

import java.util.Collection;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class CountedStream<T> {

	private final Flux<T> stream;
	private final long count;

	public CountedStream(Flux<T> stream, long count) {
		this.stream = stream;
		this.count = count;
	}

	public Flux<T> getStream() {
		return stream;
	}

	public long getCount() {
		return count;
	}

	@SafeVarargs
	public static <T> CountedStream<T> merge(CountedStream<T>... stream) {
		return merge(List.of(stream));
	}

	public static <T> CountedStream<T> merge(Collection<CountedStream<T>> stream) {
		return stream
				.stream()
				.reduce((a, b) -> new CountedStream<>(Flux.merge(a.getStream(), b.getStream()), a.getCount() + b.getCount()))
				.orElseGet(() -> new CountedStream<>(Flux.empty(), 0));
	}

	public static <T> Mono<CountedStream<T>> merge(Flux<CountedStream<T>> stream) {
		return stream
				.reduce((a, b) -> new CountedStream<>(Flux.merge(a.getStream(), b.getStream()), a.getCount() + b.getCount()))
				.switchIfEmpty(Mono.fromSupplier(() -> new CountedStream<>(Flux.empty(), 0)));
	}

	public Mono<List<T>> collectList() {
		return stream.collectList();
	}

	public static <T> Mono<CountedStream<T>> counted(Flux<T> flux) {
		var publishedFlux = flux.cache();
		return publishedFlux.count().map(count -> new CountedStream<>(publishedFlux, count));
	}
}
