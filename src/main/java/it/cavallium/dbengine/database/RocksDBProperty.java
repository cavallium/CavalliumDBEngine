package it.cavallium.dbengine.database;

public interface RocksDBProperty {

	String getName();

	boolean isNumeric();

	boolean isMap();

	boolean isString();
}
