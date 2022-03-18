package it.cavallium.dbengine.database;

import java.util.List;

public record LLSoftUpdateDocument(List<LLItem> items, List<LLItem> softDeleteItems) implements LLIndexRequest {}
