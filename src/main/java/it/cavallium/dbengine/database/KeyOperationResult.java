package it.cavallium.dbengine.database;

public record KeyOperationResult<T>(T key, boolean changed) {}
