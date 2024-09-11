module dbengine.tests {
	requires org.junit.jupiter.api;
	requires dbengine;
	requires it.cavallium.datagen;
	requires org.assertj.core;
	requires it.unimi.dsi.fastutil;
	requires org.jetbrains.annotations;
	requires micrometer.core;
	requires org.junit.jupiter.params;
	requires com.google.common;
	requires org.apache.logging.log4j;
	requires org.apache.logging.log4j.core;
	requires org.apache.commons.lang3;
	requires rocksdbjni;
	opens it.cavallium.dbengine.tests;
}