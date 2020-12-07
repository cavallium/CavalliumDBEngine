package it.cavallium.dbengine.database.remote.client;

import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.proto.CavalliumDBEngineServiceGrpc;
import it.cavallium.dbengine.proto.SingletonMethodGetRequest;
import it.cavallium.dbengine.proto.SingletonMethodSetRequest;
import java.io.IOException;
import org.jetbrains.annotations.Nullable;

public class LLRemoteSingleton implements LLSingleton {

	private final CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceBlockingStub blockingStub;
	private final int handle;
	private final String databaseName;

	public LLRemoteSingleton(
			String databaseName,
			CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceBlockingStub blockingStub, int handle) {
		this.databaseName = databaseName;
		this.blockingStub = blockingStub;
		this.handle = handle;
	}

	@Override
	public byte[] get(@Nullable LLSnapshot snapshot) throws IOException {
		try {
			var request = SingletonMethodGetRequest.newBuilder()
					.setSingletonHandle(handle);
			if (snapshot != null) {
				request.setSequenceNumber(snapshot.getSequenceNumber());
			}
			var response = blockingStub.singletonMethodGet(request.build());
			return response.getValue().toByteArray();
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public void set(byte[] value) throws IOException {
		try {
			//noinspection ResultOfMethodCallIgnored
			blockingStub.singletonMethodSet(SingletonMethodSetRequest.newBuilder()
					.setSingletonHandle(handle)
					.setValue(ByteString.copyFrom(value))
					.build());
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}
}
