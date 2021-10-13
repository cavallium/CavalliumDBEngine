package it.cavallium.dbengine;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class SyncUtils {

	public static void run(Flux<?> publisher) {
		publisher.subscribeOn(Schedulers.immediate()).blockLast();
	}

	public static void runVoid(Mono<Void> publisher) {
		publisher.then().subscribeOn(Schedulers.immediate()).block();
	}

	public static <T> T run(Mono<T> publisher) {
		return publisher.subscribeOn(Schedulers.immediate()).block();
	}

	public static <T> T run(boolean shouldFail, Mono<T> publisher) {
		return publisher.subscribeOn(Schedulers.immediate()).transform(mono -> {
			if (shouldFail) {
				return mono.onErrorResume(ex -> Mono.empty());
			} else {
				return mono;
			}
		}).block();
	}

	public static void runVoid(boolean shouldFail, Mono<Void> publisher) {
		publisher.then().subscribeOn(Schedulers.immediate()).transform(mono -> {
			if (shouldFail) {
				return mono.onErrorResume(ex -> Mono.empty());
			} else {
				return mono;
			}
		}).block();
	}
}
