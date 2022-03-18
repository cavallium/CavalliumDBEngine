package it.cavallium.dbengine.database;

import java.util.List;

public record LLUpdateDocument(List<LLItem> items) implements LLIndexRequest {}
