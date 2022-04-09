package it.cavallium.dbengine.database;

import org.rocksdb.TableProperties;

public record TableWithProperties(String name, TableProperties properties) {}
