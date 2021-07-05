package it.cavallium.dbengine.client;

import java.util.Objects;
import java.util.StringJoiner;
import reactor.core.publisher.Mono;

public record SearchResultKey<T>(Mono<T> key, float score) {}
