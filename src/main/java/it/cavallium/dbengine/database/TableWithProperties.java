package it.cavallium.dbengine.database;

import org.rocksdb.TableProperties;

public record TableWithProperties(String column, String table, TableProperties properties) {}
