package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.Column;
import it.unimi.dsi.fastutil.bytes.ByteList;
import org.jetbrains.annotations.Nullable;

public record BadBlock(String databaseName, @Nullable Column column, @Nullable ByteList rawKey,
											 @Nullable Throwable ex) {}
