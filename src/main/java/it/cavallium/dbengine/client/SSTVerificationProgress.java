package it.cavallium.dbengine.client;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.DbProgress.DbSSTProgress;
import it.cavallium.dbengine.client.SSTProgress.SSTOk;
import it.cavallium.dbengine.client.SSTProgress.SSTProgressReport;
import it.cavallium.dbengine.client.SSTProgress.SSTStart;
import it.cavallium.dbengine.client.SSTVerificationProgress.SSTBlockBad;
import it.cavallium.dbengine.rpc.current.data.Column;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public sealed interface SSTVerificationProgress extends SSTProgress permits SSTOk, SSTProgressReport, SSTStart,
		SSTBlockBad {

	record SSTBlockBad(Buf rawKey, Throwable ex) implements SSTVerificationProgress {}
}
