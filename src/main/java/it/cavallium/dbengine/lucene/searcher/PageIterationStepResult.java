package it.cavallium.dbengine.lucene.searcher;

import org.jetbrains.annotations.Nullable;

record PageIterationStepResult(CurrentPageInfo nextPageToIterate, @Nullable PageData pageData) {}
