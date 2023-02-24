package it.cavallium.dbengine.database;

import it.cavallium.dbengine.buffers.Buf;

public record SerializedKey<T>(T key, Buf serialized) {}
