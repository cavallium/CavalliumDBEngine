package it.cavallium.dbengine.lucene.collector;

import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.lucene.FullDocs;
import it.cavallium.dbengine.lucene.LLDoc;
import it.cavallium.dbengine.lucene.LLFieldDoc;
import it.cavallium.dbengine.utils.SimpleResource;
import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import reactor.core.publisher.Flux;

public class FullFieldDocs<T extends LLDoc> extends SimpleResource implements FullDocs<T>, DiscardingCloseable {

	private final FullDocs<T> fullDocs;
	private final SortField[] fields;

	public FullFieldDocs(FullDocs<T> fullDocs, SortField[] fields) {
		this.fullDocs = fullDocs;
		this.fields = fields;
	}

	@Override
	public Flux<T> iterate() {
		return fullDocs.iterate();
	}

	@Override
	public Flux<T> iterate(long skips) {
		return fullDocs.iterate(skips);
	}

	@Override
	public TotalHits totalHits() {
		return fullDocs.totalHits();
	}

	public SortField[] fields() {
		return fields;
	}

	@Override
	protected void onClose() {
		fullDocs.close();
	}
}
