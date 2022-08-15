package it.cavallium.dbengine.client;

import reactor.core.publisher.Mono;

public interface IBackuppable {

	Mono<Void> pauseForBackup();

	Mono<Void> resumeAfterBackup();

	boolean isPaused();
}
