package it.cavallium.dbengine.database;

public record LLUpdateFields(LLItem[] items) implements LLIndexRequest {}
