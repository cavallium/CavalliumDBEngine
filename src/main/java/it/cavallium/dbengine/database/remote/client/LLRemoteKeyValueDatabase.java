package it.cavallium.dbengine.database.remote.client;

import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLDeepDictionary;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.proto.DatabaseCloseRequest;
import it.cavallium.dbengine.proto.DatabaseSnapshotReleaseRequest;
import it.cavallium.dbengine.proto.DatabaseSnapshotTakeRequest;
import it.cavallium.dbengine.proto.DictionaryOpenRequest;
import it.cavallium.dbengine.proto.SingletonOpenRequest;
import it.cavallium.dbengine.proto.CavalliumDBEngineServiceGrpc;

public class LLRemoteKeyValueDatabase implements LLKeyValueDatabase {

	private final String name;
	private final DbClientFunctions clientFunctions;
	private final CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceBlockingStub blockingStub;
	private final CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceStub stub;
	private final int handle;

	public LLRemoteKeyValueDatabase(String name, DbClientFunctions clientFunctions, int handle) {
		this.name = name;
		this.clientFunctions = clientFunctions;
		this.blockingStub = clientFunctions.getBlockingStub();
		this.stub = clientFunctions.getStub();
		this.handle = handle;
	}

	@Override
	public String getDatabaseName() {
		return name;
	}

	@Override
	public LLSingleton getSingleton(byte[] singletonListColumnName, byte[] name, byte[] defaultValue)
			throws IOException {
		try {
			var response = blockingStub.singletonOpen(SingletonOpenRequest.newBuilder()
					.setDatabaseHandle(this.handle)
					.setSingletonListColumnName(ByteString.copyFrom(singletonListColumnName))
					.setName(ByteString.copyFrom(name))
					.setDefaultValue(ByteString.copyFrom(defaultValue))
					.build());
			int handle = response.getHandle();
			return new LLRemoteSingleton(LLRemoteKeyValueDatabase.this.name, blockingStub, handle);
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public LLDictionary getDictionary(byte[] columnName) throws IOException {
		try {
			var response = blockingStub.dictionaryOpen(DictionaryOpenRequest.newBuilder()
					.setDatabaseHandle(this.handle)
					.setColumnName(ByteString.copyFrom(columnName))
					.build());
			int handle = response.getHandle();
			return new LLRemoteDictionary(clientFunctions, handle, name);
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public LLDeepDictionary getDeepDictionary(byte[] columnName, int keySize, int key2Size) throws IOException {
		try {
			var response = blockingStub.dictionaryOpen(DictionaryOpenRequest.newBuilder()
					.setDatabaseHandle(this.handle)
					.setColumnName(ByteString.copyFrom(columnName))
					.build());
			int handle = response.getHandle();
			throw new UnsupportedOperationException("Deep dictionaries are not implemented in remote databases!"); //todo: implement
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public LLSnapshot takeSnapshot() throws IOException {
		try {
			var response = blockingStub.databaseSnapshotTake(DatabaseSnapshotTakeRequest.newBuilder()
					.setDatabaseHandle(this.handle)
					.build());
			long sequenceNumber = response.getSequenceNumber();
			return new LLSnapshot(sequenceNumber);
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public void releaseSnapshot(LLSnapshot snapshot) throws IOException {
		try {
			var response = blockingStub.databaseSnapshotRelease(DatabaseSnapshotReleaseRequest.newBuilder()
					.setDatabaseHandle(this.handle)
					.setSequenceNumber(snapshot.getSequenceNumber())
					.build());
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public long getProperty(String propertyName) throws IOException {
		throw new UnsupportedOperationException("Not implemented"); //todo: implement
	}

	@Override
	public void close() throws IOException {
		try {
			var response = blockingStub.databaseClose(DatabaseCloseRequest.newBuilder()
					.setDatabaseHandle(this.handle)
					.build());
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}
}
