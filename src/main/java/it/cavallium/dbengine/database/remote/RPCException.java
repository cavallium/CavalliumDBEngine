package it.cavallium.dbengine.database.remote;

import org.jetbrains.annotations.Nullable;

public class RPCException extends RuntimeException {

	public RPCException(int code, @Nullable String message) {
		super("RPC error " + code + (message != null ? (": " + message) : ""));
	}
}
