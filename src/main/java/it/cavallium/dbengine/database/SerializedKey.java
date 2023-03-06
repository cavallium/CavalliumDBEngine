package it.cavallium.dbengine.database;

import it.cavallium.buffer.Buf;

public record SerializedKey<T>(T key, Buf serialized) {}
