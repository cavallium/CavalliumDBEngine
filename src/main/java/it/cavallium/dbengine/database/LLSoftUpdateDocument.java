package it.cavallium.dbengine.database;

public record LLSoftUpdateDocument(LLItem[] items, LLItem[] softDeleteItems) implements LLIndexRequest {}
