package it.cavallium.dbengine.database;

import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public interface LLSingleton extends LLKeyValueDatabaseStructure {

	Mono<byte[]> get(@Nullable LLSnapshot snapshot);

	Mono<Void> set(byte[] value);
}
