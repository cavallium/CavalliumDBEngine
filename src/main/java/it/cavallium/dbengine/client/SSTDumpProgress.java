package it.cavallium.dbengine.client;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.SSTDumpProgress.SSTBlockFail;
import it.cavallium.dbengine.client.SSTDumpProgress.SSTBlockKeyValue;
import it.cavallium.dbengine.client.SSTProgress.SSTOk;
import it.cavallium.dbengine.client.SSTProgress.SSTProgressReport;
import it.cavallium.dbengine.client.SSTProgress.SSTStart;
import org.rocksdb.RocksDBException;

public sealed interface SSTDumpProgress extends SSTProgress permits SSTBlockFail, SSTBlockKeyValue, SSTOk,
		SSTProgressReport, SSTStart {

	record SSTBlockKeyValue(Buf rawKey, Buf rawValue) implements SSTDumpProgress {}

	record SSTBlockFail(RocksDBException ex) implements SSTDumpProgress {}
}
