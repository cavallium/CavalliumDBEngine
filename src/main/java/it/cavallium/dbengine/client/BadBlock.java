package it.cavallium.dbengine.client;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.rpc.current.data.Column;
import org.jetbrains.annotations.Nullable;

public record BadBlock(String databaseName, @Nullable Column column, @Nullable Buf rawKey,
											 @Nullable Throwable ex) {}
