package it.cavallium.dbengine.database;

import java.util.concurrent.ForkJoinPool;

public interface LLKeyValueDatabaseStructure {

	String getDatabaseName();

	ForkJoinPool getDbReadPool();

	ForkJoinPool getDbWritePool();
}
