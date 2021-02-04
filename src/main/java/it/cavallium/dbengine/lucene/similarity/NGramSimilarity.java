/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.cavallium.dbengine.lucene.similarity;


import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.novasearch.lucene.search.similarities.BM25Similarity;
import org.novasearch.lucene.search.similarities.BM25Similarity.BM25Model;

public class NGramSimilarity {

  private NGramSimilarity() {

  }

  public static Similarity classic() {
    var instance = new ClassicSimilarity();
    instance.setDiscountOverlaps(false);
    return instance;
  }

  public static Similarity bm25(BM25Model model) {
    var instance = new BM25Similarity(model);
    instance.setDiscountOverlaps(false);
    return instance;
  }

  public static Similarity bm15(BM25Model model) {
    var instance = new BM25Similarity(1.2f, 0.0f, 0.5f, model);
    instance.setDiscountOverlaps(false);
    return instance;
  }

  public static Similarity bm11(BM25Model model) {
    var instance = new BM25Similarity(1.2f, 1.0f, 0.5f, model);
    instance.setDiscountOverlaps(false);
    return instance;
  }
}
