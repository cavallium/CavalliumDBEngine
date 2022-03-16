package it.cavallium.dbengine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import io.netty5.buffer.ByteBuf;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.LLScoreDoc;
import it.cavallium.dbengine.lucene.LMDBSortedCodec;
import it.cavallium.dbengine.lucene.LMDBPriorityQueue;
import it.cavallium.dbengine.lucene.PriorityQueue;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.ScoreDoc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

public class TestLMDBHitQueue {

	public static final int NUM_HITS = 1024;

	private LLTempLMDBEnv env;
	private SafeCloseable lmdbQueue;

	private TestingPriorityQueue testingPriorityQueue;

	protected static boolean lessThan(ScoreDoc hitA, ScoreDoc hitB) {
		if (hitA.score == hitB.score) {
			return hitA.doc > hitB.doc;
		} else {
			return hitA.score < hitB.score;
		}
	}

	private static int compareScoreDoc(ScoreDoc hitA, ScoreDoc hitB) {
		if (hitA.score == hitB.score) {
			if (hitA.doc == hitB.doc) {
				return Integer.compare(hitA.shardIndex, hitB.shardIndex);
			} else {
				return Integer.compare(hitB.doc, hitA.doc);
			}
		} else {
			return Float.compare(hitA.score, hitB.score);
		}
	}

	private static void assertEqualsScoreDoc(ScoreDoc expected, ScoreDoc actual) {
		Assertions.assertEquals(toLLScoreDoc(expected), toLLScoreDoc(actual));
	}

	private static void assertEqualsScoreDoc(List<ScoreDoc> expected, List<ScoreDoc> actual) {
		Assertions.assertEquals(expected.size(), actual.size());
		var list1 = expected.iterator();
		var list2 = actual.iterator();
		while (list1.hasNext() && list2.hasNext()) {
			Assertions.assertFalse(lessThan(list1.next(), list2.next()));
		}
	}

	@BeforeEach
	public void beforeEach() throws IOException {
		this.env = new LLTempLMDBEnv();
		var lmdbQueue = new LMDBPriorityQueue<ScoreDoc>(env, new LMDBSortedCodec<>() {

			@Override
			public ByteBuf serialize(Function<Integer, ByteBuf> allocator, ScoreDoc data) {
				var buf = allocator.apply(Float.BYTES + Integer.BYTES + Integer.BYTES);
				setScore(buf, data.score);
				setDoc(buf, data.doc);
				setShardIndex(buf, data.shardIndex);
				buf.writerIndex(Float.BYTES + Integer.BYTES + Integer.BYTES);
				return buf.asReadOnly();
			}

			@Override
			public ScoreDoc deserialize(ByteBuf buf) {
				return new ScoreDoc(getDoc(buf), getScore(buf), getShardIndex(buf));
			}

			@Override
			public int compare(ScoreDoc hitA, ScoreDoc hitB) {
				return compareScoreDoc(hitA, hitB);
			}

			@Override
			public int compareDirect(ByteBuf hitA, ByteBuf hitB) {
				var scoreA = getScore(hitA);
				var scoreB = getScore(hitB);
				if (scoreA == scoreB) {
					var docA = getDoc(hitA);
					var docB = getDoc(hitB);
					if (docA == docB) {
						return Integer.compare(getShardIndex(hitA), getShardIndex(hitB));
					} else {
						return Integer.compare(docB, docA);
					}
				} else {
					return Float.compare(scoreA, scoreB);
				}
			}

			private static float getScore(ByteBuf hit) {
				return hit.getFloat(0);
			}

			private static int getDoc(ByteBuf hit) {
				return hit.getInt(Float.BYTES);
			}

			private static int getShardIndex(ByteBuf hit) {
				return hit.getInt(Float.BYTES + Integer.BYTES);
			}

			private static void setScore(ByteBuf hit, float score) {
				hit.setFloat(0, score);
			}

			private static void setDoc(ByteBuf hit, int doc) {
				hit.setInt(Float.BYTES, doc);
			}

			private static void setShardIndex(ByteBuf hit, int shardIndex) {
				hit.setInt(Float.BYTES + Integer.BYTES, shardIndex);
			}
		});
		this.lmdbQueue = lmdbQueue;
		PriorityQueueAdaptor<ScoreDoc> hitQueue = new PriorityQueueAdaptor<>(new HitQueue(NUM_HITS, false));
		Assertions.assertEquals(0, lmdbQueue.size());
		Assertions.assertEquals(0, hitQueue.size());
		this.testingPriorityQueue = new TestingPriorityQueue(hitQueue, lmdbQueue);
	}

	@Test
	public void testNoOp() {

	}

	@Test
	public void testEmptyTop() {
		Assertions.assertNull(testingPriorityQueue.top());
	}

	@Test
	public void testAddSingle() {
		var item = new ScoreDoc(0, 0, 0);
		testingPriorityQueue.add(item);
		assertEqualsScoreDoc(item, testingPriorityQueue.top());
	}

	@Test
	public void testAddMulti() {
		for (int i = 0; i < 1000; i++) {
			var item = new ScoreDoc(i, i >> 1, -1);
			testingPriorityQueue.addUnsafe(item);
		}
		assertEqualsScoreDoc(new ScoreDoc(1, 0, -1), testingPriorityQueue.top());
	}

	@Test
	public void testAddMultiClear() {
		for (int i = 0; i < 1000; i++) {
			var item = new ScoreDoc(i, i >> 1, -1);
			testingPriorityQueue.addUnsafe(item);
		}
		testingPriorityQueue.clear();
		Assertions.assertNull(testingPriorityQueue.top());
	}

	@Test
	public void testAddRemove() {
		var item = new ScoreDoc(0, 0, -1);
		testingPriorityQueue.add(item);
		testingPriorityQueue.remove(item);
		Assertions.assertNull(testingPriorityQueue.top());
	}

	@Test
	public void testAddRemoveNonexistent() {
		var item = new ScoreDoc(0, 0, 0);
		testingPriorityQueue.addUnsafe(item);
		testingPriorityQueue.remove(new ScoreDoc(2, 0, 0));
		assertEqualsScoreDoc(item, testingPriorityQueue.top());
	}

	@Test
	public void testAddMultiRemove1() {
		ScoreDoc toRemove = null;
		ScoreDoc top = null;
		for (int i = 0; i < 1000; i++) {
			var item = new ScoreDoc(i, i >> 1, -1);
			if (i == 1) {
				toRemove = item;
			} else if (i == 0) {
				top = item;
			}
			testingPriorityQueue.addUnsafe(item);
		}
		testingPriorityQueue.removeUnsafe(toRemove);
		assertEqualsScoreDoc(top, testingPriorityQueue.top());
	}

	@Test
	public void testAddMultiRemove2() {
		ScoreDoc toRemove = null;
		ScoreDoc top = null;
		for (int i = 0; i < 1000; i++) {
			var item = new ScoreDoc(i, i >> 1, -1);
			if (i == 0) {
				toRemove = item;
			} else if (i == 1) {
				top = item;
			}
			testingPriorityQueue.addUnsafe(item);
		}
		testingPriorityQueue.removeUnsafe(new ScoreDoc(0, 0, -1));
		assertEqualsScoreDoc(top, testingPriorityQueue.top());
	}

	@Test
	public void testSort() {
		var sortedNumbers = new ArrayList<ScoreDoc>();
		for (int i = 0; i < 1000; i++) {
			sortedNumbers.add(new ScoreDoc(i, i >> 1, -1));
		}
		sortedNumbers.sort(TestLMDBHitQueue::compareScoreDoc);
		var shuffledNumbers = new ArrayList<>(sortedNumbers);
		Collections.shuffle(shuffledNumbers, new Random(1000));
		for (ScoreDoc scoreDoc : shuffledNumbers) {
			testingPriorityQueue.addUnsafe(scoreDoc);
		}

		var newSortedNumbers = new ArrayList<ScoreDoc>();
		ScoreDoc popped;
		while ((popped = testingPriorityQueue.popUnsafe()) != null) {
			newSortedNumbers.add(popped);
		}

		assertEqualsScoreDoc(sortedNumbers, newSortedNumbers);
	}

	@AfterEach
	public void afterEach() throws IOException {
		lmdbQueue.close();
		assertEquals(0, env.countUsedDbs());
		env.close();
	}

	private static class TestingPriorityQueue implements PriorityQueue<ScoreDoc> {

		private final PriorityQueue<ScoreDoc> referenceQueue;
		private final PriorityQueue<ScoreDoc> testQueue;

		public TestingPriorityQueue(PriorityQueue<ScoreDoc> referenceQueue, PriorityQueue<ScoreDoc> testQueue) {
			this.referenceQueue = referenceQueue;
			this.testQueue = testQueue;
		}

		@Override
		public void add(ScoreDoc element) {
			referenceQueue.add(element);
			testQueue.add(element);
			ensureEquality();
		}

		public void addUnsafe(ScoreDoc element) {
			referenceQueue.add(element);
			testQueue.add(element);
		}

		@Override
		public ScoreDoc top() {
			var top1 = referenceQueue.top();
			var top2 = testQueue.top();
			assertEqualsScoreDoc(top1, top2);
			return top2;
		}

		public ScoreDoc topUnsafe() {
			var top1 = referenceQueue.top();
			var top2 = testQueue.top();
			return top2;
		}

		@Override
		public ScoreDoc pop() {
			var top1 = referenceQueue.pop();
			var top2 = testQueue.pop();
			assertEqualsScoreDoc(top1, top2);
			return top2;
		}

		public ScoreDoc popUnsafe() {
			var top1 = referenceQueue.pop();
			var top2 = testQueue.pop();
			return top2;
		}

		@Override
		public void replaceTop(ScoreDoc newTop) {
			referenceQueue.replaceTop(newTop);
			testQueue.replaceTop(newTop);
		}

		@Override
		public long size() {
			var size1 = referenceQueue.size();
			var size2 = testQueue.size();
			Assertions.assertEquals(size1, size2);
			return size2;
		}

		@Override
		public void clear() {
			referenceQueue.clear();
			testQueue.clear();
		}

		@Override
		public boolean remove(ScoreDoc element) {
			var removed1 = referenceQueue.remove(element);
			var removed2 = testQueue.remove(element);
			Assertions.assertEquals(removed1, removed2);
			return removed2;
		}

		public boolean  removeUnsafe(ScoreDoc element) {
			var removed1 = referenceQueue.remove(element);
			var removed2 = testQueue.remove(element);
			return removed2;
		}

		@Override
		public Flux<ScoreDoc> iterate() {
			//noinspection BlockingMethodInNonBlockingContext
			var it1 = referenceQueue.iterate().collectList().blockOptional().orElseThrow();
			//noinspection BlockingMethodInNonBlockingContext
			var it2 = testQueue.iterate().collectList().blockOptional().orElseThrow();
			assertEqualsScoreDoc(it1, it2);
			return Flux.fromIterable(it2);
		}

		@Override
		public void close() {
			referenceQueue.close();
			testQueue.close();
		}

		private void ensureEquality() {
			Assertions.assertEquals(referenceQueue.size(), testQueue.size());
			var referenceQueueElements = Lists.newArrayList(referenceQueue
					.iterate()
					.map(TestLMDBHitQueue::toLLScoreDoc)
					.toIterable());
			var testQueueElements = Lists.newArrayList(testQueue
					.iterate()
					.map(TestLMDBHitQueue::toLLScoreDoc)
					.toIterable());
			Assertions.assertEquals(referenceQueueElements, testQueueElements);
		}
	}

	public static LLScoreDoc toLLScoreDoc(ScoreDoc scoreDoc) {
		if (scoreDoc == null) return null;
		return new LLScoreDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
	}
}
