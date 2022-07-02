package it.cavallium.dbengine.lucene.mlt;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.AlreadyClosedException;

public class BigCompositeReader<R extends IndexReader> {

	private static final long ACTUAL_MAX_DOCS = Long.MAX_VALUE - 10;
	private final R[] subReaders;
	protected final Comparator<R> subReadersSorter;
	private final long[] starts;
	private final long maxDoc;
	private final AtomicLong numDocs = new AtomicLong(-1);
	private final List<R> subReadersList;

	public BigCompositeReader(R subReader, IntFunction<R[]> arrayInstantiation) {
		this(toArray(subReader, arrayInstantiation), null);
	}

	private static <R extends IndexReader> R[] toArray(R subReader, IntFunction<R[]> arrayInstantiation) {
		var arr = arrayInstantiation.apply(1);
		arr[0] = subReader;
		return arr;
	}

	public BigCompositeReader(R[] subReaders, Comparator<R> subReadersSorter) {
		if (subReadersSorter != null) {
			Arrays.sort(subReaders, subReadersSorter);
		}

		this.subReaders = subReaders;
		this.subReadersSorter = subReadersSorter;
		this.subReadersList = List.of(subReaders);
		this.starts = new long[subReaders.length + 1];
		BigInteger maxDoc = BigInteger.ZERO;

		for(int i = 0; i < subReaders.length; ++i) {
			this.starts[i] = maxDoc.longValue();
			IndexReader r = subReaders[i];
			maxDoc = maxDoc.add(BigInteger.valueOf(r.maxDoc()));
		}

		if (maxDoc.compareTo(BigInteger.ZERO) < 0 || maxDoc.compareTo(BigInteger.valueOf(ACTUAL_MAX_DOCS)) > 0) {
			throw new IllegalArgumentException("Too many documents: composite IndexReaders cannot exceed "
					+ ACTUAL_MAX_DOCS + " but readers have total maxDoc=" + maxDoc);
		} else {
			this.maxDoc = maxDoc.longValueExact();
			this.starts[subReaders.length] = this.maxDoc;
		}
	}

	public static <T extends IndexReader> Collection<String> getIndexedFields(BigCompositeReader<T> readers) {
		return readers.subReadersList
				.stream()
				.map(t -> t.getContext())
				.flatMap(l -> l.leaves().stream())
				.flatMap((l) -> StreamSupport
						.stream(l.reader().getFieldInfos().spliterator(), false)
						.filter((fi) -> fi.getIndexOptions() != IndexOptions.NONE))
				.map((fi) -> fi.name)
				.collect(Collectors.toSet());
	}

	private void ensureOpen() {
		for (R subReader : subReaders) {
			if (subReader.getRefCount() <= 0) {
				throw new AlreadyClosedException("this IndexReader is closed");
			}
		}
	}

	public long getDocCount(String field) throws IOException {
		this.ensureOpen();
		long total = 0;

		for (R reader : this.subReaders) {
			int sub = reader.getDocCount(field);

			assert sub >= 0;

			assert sub <= reader.maxDoc();

			total += sub;
		}

		return total;
	}

	public long docFreq(Term term) throws IOException {
		this.ensureOpen();
		long total = 0;

		for (R subReader : this.subReaders) {
			int sub = subReader.docFreq(term);

			assert sub >= 0;

			assert sub <= subReader.getDocCount(term.field());

			total += sub;
		}

		return total;
	}

	public long numDocs() {
		long numDocs = this.numDocs.getOpaque();
		if (numDocs == -1L) {
			numDocs = 0L;

			for (IndexReader r : this.subReaders) {
				numDocs += r.numDocs();
			}

			assert numDocs >= 0L;

			this.numDocs.set(numDocs);
		}

		return numDocs;
	}

	public Fields getTermVectors(long docID) throws IOException {
		this.ensureOpen();
		int i = this.readerIndex(docID);
		return this.subReaders[i].getTermVectors(Math.toIntExact(docID - this.starts[i]));
	}

	protected final int readerIndex(long docID) {
		if (docID >= 0 && docID < this.maxDoc) {
			return subIndex(docID, this.starts);
		} else {
			throw new IllegalArgumentException("docID must be >= 0 and < maxDoc=" + this.maxDoc + " (got docID=" + docID + ")");
		}
	}

	public static int subIndex(long n, long[] docStarts) {
		int size = docStarts.length;
		int lo = 0;
		int hi = size - 1;

		while(hi >= lo) {
			int mid = lo + hi >>> 1;
			long midValue = docStarts[mid];
			if (n < midValue) {
				hi = mid - 1;
			} else {
				if (n <= midValue) {
					while(mid + 1 < size && docStarts[mid + 1] == midValue) {
						++mid;
					}

					return mid;
				}

				lo = mid + 1;
			}
		}

		return hi;
	}

	public final void document(long docID, StoredFieldVisitor visitor) throws IOException {
		this.ensureOpen();
		int i = this.readerIndex(docID);
		this.subReaders[i].document(Math.toIntExact(docID - this.starts[i]), visitor);
	}

	public final Document document(long docID) throws IOException {
		DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
		this.document(docID, visitor);
		return visitor.getDocument();
	}

	public final Document document(long docID, Set<String> fieldsToLoad) throws IOException {
		DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor(fieldsToLoad);
		this.document(docID, visitor);
		return visitor.getDocument();
	}

	public long maxDoc() {
		return this.maxDoc;
	}
}
