package it.cavallium.dbengine.lucene;

import it.cavallium.dbengine.database.SafeCloseable;

/**
 * This closeable should be run on a lucene thread
 */
public interface LuceneCloseable extends SafeCloseable {}
