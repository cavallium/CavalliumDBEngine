package it.cavallium.dbengine.database;

import org.jetbrains.annotations.Nullable;

public record Delta<T>(@Nullable T previous, @Nullable T current) {}
