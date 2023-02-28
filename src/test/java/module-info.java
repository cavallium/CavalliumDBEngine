module dbengine.tests {
	requires org.junit.jupiter.api;
	requires dbengine;
	requires data.generator.runtime;
	requires org.assertj.core;
	requires org.apache.lucene.core;
	requires it.unimi.dsi.fastutil;
	requires org.apache.lucene.queryparser;
	requires org.jetbrains.annotations;
	requires micrometer.core;
	requires org.junit.jupiter.params;
	requires com.google.common;
	requires org.apache.logging.log4j;
	requires org.apache.commons.lang3;
	requires rocksdbjni;
	opens it.cavallium.dbengine.tests;
}