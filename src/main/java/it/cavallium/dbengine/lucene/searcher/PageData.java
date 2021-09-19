package it.cavallium.dbengine.lucene.searcher;

import org.apache.lucene.search.TopDocs;

record PageData(TopDocs topDocs, CurrentPageInfo nextPageInfo) {}
