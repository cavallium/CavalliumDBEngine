package it.cavallium.dbengine.database;

public record LLUpdateDocument(LLItem[] items) implements LLIndexRequest {}
