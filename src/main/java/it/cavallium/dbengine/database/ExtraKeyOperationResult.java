package it.cavallium.dbengine.database;

public record ExtraKeyOperationResult<T, X>(T key, X extra, boolean changed) {}
