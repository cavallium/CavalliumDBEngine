package it.cavallium.dbengine.database;

public record ColumnProperty<T>(String columnName, String propertyName, T value) {}
