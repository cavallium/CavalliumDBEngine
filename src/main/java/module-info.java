module dbengine {
	exports it.cavallium.dbengine.lucene;
	exports it.cavallium.dbengine.database;
	exports it.cavallium.dbengine.rpc.current.data;
	exports it.cavallium.dbengine.database.remote;
	exports it.cavallium.dbengine.database.disk;
	exports it.cavallium.dbengine.rpc.current.data.nullables;
	exports it.cavallium.dbengine.database.serialization;
	exports it.cavallium.dbengine.client;
	exports it.cavallium.dbengine.client.query.current.data;
	exports it.cavallium.dbengine.lucene.collector;
	exports it.cavallium.dbengine.lucene.searcher;
	exports it.cavallium.dbengine.database.collections;
	exports it.cavallium.dbengine.lucene.analyzer;
	exports it.cavallium.dbengine.client.query;
	exports it.cavallium.dbengine.database.memory;
	opens it.cavallium.dbengine.database.remote;
	exports it.cavallium.dbengine.utils;
	exports it.cavallium.dbengine.database.disk.rocksdb;
	exports it.cavallium.dbengine.lucene.hugepq.search;
	requires org.jetbrains.annotations;
	requires com.google.common;
	requires micrometer.core;
	requires rocksdbjni;
	requires org.apache.logging.log4j;
	requires static io.soabase.recordbuilder.core;
	requires it.unimi.dsi.fastutil;
	requires data.generator.runtime;
	requires java.logging;
	requires org.apache.lucene.core;
	requires org.apache.commons.lang3;
	requires java.compiler;
	requires org.apache.lucene.analysis.common;
	requires org.apache.lucene.misc;
	requires org.apache.lucene.codecs;
	requires org.apache.lucene.backward_codecs;
	requires lucene.relevance;
	requires org.apache.lucene.facet;
	requires java.management;
	requires com.ibm.icu;
	requires org.apache.lucene.analysis.icu;
	requires org.apache.lucene.queryparser;
	requires okio;
	requires moshi.records.reflect;
	requires moshi;
	requires static jdk.unsupported;
}