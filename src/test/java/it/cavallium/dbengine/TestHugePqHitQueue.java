package it.cavallium.dbengine;

import com.google.common.collect.Lists;
import io.netty5.buffer.Buffer;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.lucene.LLScoreDoc;
import it.cavallium.dbengine.lucene.HugePqCodec;
import it.cavallium.dbengine.lucene.HugePqPriorityQueue;
import it.cavallium.dbengine.lucene.PriorityQueue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.ScoreDoc;
import org.assertj.core.description.Description;
import org.assertj.core.description.TextDescription;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

public class TestHugePqHitQueue {

	public static final int NUM_HITS = 1024;

	private LLTempHugePqEnv env;
	private SafeCloseable hugePqQueue;

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

	private static void assertEqualsScoreDoc(Description description, ScoreDoc expected, ScoreDoc actual) {
		org.assertj.core.api.Assertions.assertThat(toLLScoreDoc(expected)).as(description).isEqualTo(toLLScoreDoc(actual));
	}

	private static void assertEqualsScoreDoc(List<ScoreDoc> expected, List<ScoreDoc> actual) {
		var list1 = expected.iterator();
		var list2 = actual.iterator();
		Assertions.assertEquals(expected.size(), actual.size());
		while (list1.hasNext() && list2.hasNext()) {
			Assertions.assertFalse(lessThan(list1.next(), list2.next()));
		}
	}

	@BeforeEach
	public void beforeEach() throws IOException {
		this.env = new LLTempHugePqEnv();
		var hugePqQueue = new HugePqPriorityQueue<ScoreDoc>(env, new HugePqCodec<>() {

			@Override
			public Buffer serialize(Function<Integer, Buffer> allocator, ScoreDoc data) {
				var buf = allocator.apply(Float.BYTES + Integer.BYTES + Integer.BYTES);
				buf.writerOffset(Float.BYTES + Integer.BYTES + Integer.BYTES);
				setScore(buf, data.score);
				setDoc(buf, data.doc);
				setShardIndex(buf, data.shardIndex);
				return buf;
			}

			@Override
			public ScoreDoc deserialize(Buffer buf) {
				return new ScoreDoc(getDoc(buf), getScore(buf), getShardIndex(buf));
			}

			private static float getScore(Buffer hit) {
				return HugePqCodec.getLexFloat(hit, 0, false);
			}

			private static int getDoc(Buffer hit) {
				return HugePqCodec.getLexInt(hit, Float.BYTES, true);
			}

			private static int getShardIndex(Buffer hit) {
				return HugePqCodec.getLexInt(hit, Float.BYTES + Integer.BYTES, false);
			}

			private static void setScore(Buffer hit, float score) {
				HugePqCodec.setLexFloat(hit, 0, false, score);
			}

			private static void setDoc(Buffer hit, int doc) {
				HugePqCodec.setLexInt(hit, Float.BYTES, true, doc);
			}

			private static void setShardIndex(Buffer hit, int shardIndex) {
				HugePqCodec.setLexInt(hit, Float.BYTES + Integer.BYTES, false, shardIndex);
			}

			@Override
			public ScoreDoc clone(ScoreDoc obj) {
				return new ScoreDoc(obj.doc, obj.score, obj.shardIndex);
			}
		});
		this.hugePqQueue = hugePqQueue;
		PriorityQueueAdaptor<ScoreDoc> hitQueue = new PriorityQueueAdaptor<>(new HitQueue(NUM_HITS, false));
		Assertions.assertEquals(0, hugePqQueue.size());
		Assertions.assertEquals(0, hitQueue.size());
		this.testingPriorityQueue = new TestingPriorityQueue(hitQueue, hugePqQueue);
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
		assertEqualsScoreDoc(new TextDescription("top value of %s", testingPriorityQueue), item, testingPriorityQueue.top());
	}

	@Test
	public void testAddMulti() {
		for (int i = 0; i < 1000; i++) {
			var item = new ScoreDoc(i, i >> 1, -1);
			testingPriorityQueue.addUnsafe(item);
		}
		assertEqualsScoreDoc(new TextDescription("top value of %s", testingPriorityQueue), new ScoreDoc(1, 0, -1), testingPriorityQueue.top());
	}

	@Test
	public void testAddMultiRandom() {
		var list = new ArrayList<Integer>(1000);
		for (int i = 0; i < 1000; i++) {
			var ri = ThreadLocalRandom.current().nextInt(0, 20);
			list.add(ri);
			var item = new ScoreDoc(ri, ri << 1, ri % 4);
			testingPriorityQueue.addUnsafe(item);
		}
		list.sort(Comparator.reverseOrder());
		for (int i = 0; i < 1000; i++) {
			var top = list.remove(list.size() - 1);
			assertEqualsScoreDoc(new TextDescription("%d value of %s", i, testingPriorityQueue), new ScoreDoc(top, top << 1, top % 4), testingPriorityQueue.pop());
		}
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
		assertEqualsScoreDoc(new TextDescription("top value of %s", testingPriorityQueue), item, testingPriorityQueue.top());
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
		assertEqualsScoreDoc(new TextDescription("top value of %s", testingPriorityQueue), top, testingPriorityQueue.top());
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
		assertEqualsScoreDoc(new TextDescription("top value of %s", testingPriorityQueue), top, testingPriorityQueue.top());
	}

	@Test
	public void testSort() {
		var sortedNumbers = new ArrayList<ScoreDoc>();
		for (int i = 0; i < 1000; i++) {
			sortedNumbers.add(new ScoreDoc(i, i >> 1, -1));
		}
		sortedNumbers.sort(TestHugePqHitQueue::compareScoreDoc);
		var shuffledNumbers = new ArrayList<>(sortedNumbers);
		Collections.shuffle(shuffledNumbers, new Random(1000));

		org.assertj.core.api.Assertions.assertThat(testingPriorityQueue.size()).isEqualTo(0);

		for (ScoreDoc scoreDoc : shuffledNumbers) {
			testingPriorityQueue.addUnsafe(scoreDoc);
		}

		org.assertj.core.api.Assertions.assertThat(testingPriorityQueue.size()).isEqualTo(sortedNumbers.size());

		var newSortedNumbers = new ArrayList<ScoreDoc>();
		ScoreDoc popped;
		while ((popped = testingPriorityQueue.popUnsafe()) != null) {
			newSortedNumbers.add(popped);
		}
		org.assertj.core.api.Assertions.assertThat(testingPriorityQueue.size()).isEqualTo(0);

		assertEqualsScoreDoc(sortedNumbers, newSortedNumbers);
	}

	@AfterEach
	public void afterEach() throws IOException {
		hugePqQueue.close();
		env.close();
	}

	private static class TestingPriorityQueue implements PriorityQueue<ScoreDoc> {

		private final PriorityQueue<ScoreDoc> referenceQueue;
		private final PriorityQueue<ScoreDoc> myQueue;

		public TestingPriorityQueue(PriorityQueue<ScoreDoc> referenceQueue, PriorityQueue<ScoreDoc> myQueue) {
			this.referenceQueue = referenceQueue;
			this.myQueue = myQueue;
		}

		@Override
		public void add(ScoreDoc element) {
			referenceQueue.add(element);
			myQueue.add(element);
			ensureEquality();
		}

		public void addUnsafe(ScoreDoc element) {
			referenceQueue.add(element);
			myQueue.add(element);
		}

		@Override
		public ScoreDoc top() {
			var top1 = referenceQueue.top();
			var top2 = myQueue.top();
			assertEqualsScoreDoc(new TextDescription("top value of %s", myQueue), top1, top2);
			return top2;
		}

		public ScoreDoc topUnsafe() {
			var top1 = referenceQueue.top();
			var top2 = myQueue.top();
			return top2;
		}

		@Override
		public ScoreDoc pop() {
			var top1 = referenceQueue.pop();
			var top2 = myQueue.pop();
			assertEqualsScoreDoc(new TextDescription("top value of %s", myQueue), top1, top2);
			return top2;
		}

		public ScoreDoc popUnsafe() {
			var top1 = referenceQueue.pop();
			var top2 = myQueue.pop();
			return top2;
		}

		@Override
		public void replaceTop(ScoreDoc oldTop, ScoreDoc newTop) {
			referenceQueue.replaceTop(oldTop, newTop);
			myQueue.replaceTop(oldTop, newTop);
		}

		@Override
		public long size() {
			var size1 = referenceQueue.size();
			var size2 = myQueue.size();
			Assertions.assertEquals(size1, size2);
			return size2;
		}

		@Override
		public void clear() {
			referenceQueue.clear();
			myQueue.clear();
		}

		@Override
		public boolean remove(ScoreDoc element) {
			var removedRef = referenceQueue.remove(element);
			var removedMy = myQueue.remove(element);
			Assertions.assertEquals(removedRef, removedMy);
			return removedMy;
		}

		public boolean  removeUnsafe(ScoreDoc element) {
			var removed1 = referenceQueue.remove(element);
			var removed2 = myQueue.remove(element);
			return removed2;
		}

		@Override
		public Flux<ScoreDoc> iterate() {
			//noinspection BlockingMethodInNonBlockingContext
			var it1 = referenceQueue.iterate().collectList().blockOptional().orElseThrow();
			//noinspection BlockingMethodInNonBlockingContext
			var it2 = myQueue.iterate().collectList().blockOptional().orElseThrow();
			assertEqualsScoreDoc(it1, it2);
			return Flux.fromIterable(it2);
		}

		@Override
		public void close() {
			referenceQueue.close();
			myQueue.close();
		}

		private void ensureEquality() {
			Assertions.assertEquals(referenceQueue.size(), myQueue.size());
			var referenceQueueElements = Lists.newArrayList(referenceQueue
					.iterate()
					.map(TestHugePqHitQueue::toLLScoreDoc)
					.toIterable());
			var testQueueElements = Lists.newArrayList(myQueue
					.iterate()
					.map(TestHugePqHitQueue::toLLScoreDoc)
					.toIterable());
			Assertions.assertEquals(referenceQueueElements, testQueueElements);
		}
	}

	public static LLScoreDoc toLLScoreDoc(ScoreDoc scoreDoc) {
		if (scoreDoc == null) return null;
		return new LLScoreDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
	}
}
