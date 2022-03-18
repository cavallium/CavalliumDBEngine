package it.cavallium.dbengine.database;

import java.util.List;

public record LLUpdateFields(List<LLItem> items) implements LLIndexRequest {}
