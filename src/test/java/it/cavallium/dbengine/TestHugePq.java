package it.cavallium.dbengine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty5.buffer.api.Buffer;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.lucene.HugePqCodec;
import it.cavallium.dbengine.lucene.HugePqPriorityQueue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestHugePq {

	private LLTempHugePqEnv env;
	private HugePqPriorityQueue<Integer> queue;

	@BeforeEach
	public void beforeEach() throws IOException {
		this.env = new LLTempHugePqEnv();
		this.queue = new HugePqPriorityQueue<>(env, new HugePqCodec<Integer>() {
			@Override
			public Buffer serialize(Function<Integer, Buffer> allocator, Integer data) {
				return HugePqCodec.setLexInt(allocator.apply(Integer.BYTES), 0, false, data);
			}

			@Override
			public Integer deserialize(Buffer b) {
				return HugePqCodec.getLexInt(b, 0, false);
			}
		});
	}

	@Test
	public void testNoOp() {

	}

	@Test
	public void testEmptyTop() {
		Assertions.assertNull(queue.top());
	}

	@Test
	public void testAddSingle() {
		queue.add(2);
		Assertions.assertEquals(2, queue.top());
	}

	@Test
	public void testAddSame() {
		queue.add(2);
		queue.add(2);
		Assertions.assertEquals(2, queue.top());
		Assertions.assertEquals(2, queue.size());
	}

	@Test
	public void testAddMulti() {
		for (int i = 0; i < 1000; i++) {
			queue.add(i);
		}
		Assertions.assertEquals(0, queue.top());
	}

	@Test
	public void testAddMultiClear() {
		for (int i = 0; i < 1000; i++) {
			queue.add(i);
		}
		queue.clear();
		Assertions.assertNull(queue.top());
	}

	@Test
	public void testAddRemove() {
		queue.add(0);
		queue.remove(0);
		Assertions.assertNull(queue.top());
	}

	@Test
	public void testAddRemoveNonexistent() {
		queue.add(0);
		queue.remove(1);
		Assertions.assertEquals(0, queue.top());
	}

	@Test
	public void testAddMultiSameRemove() {
		queue.add(0);
		queue.add(0);
		queue.add(1);
		queue.remove(0);
		Assertions.assertEquals(2, queue.size());
		Assertions.assertEquals(0, queue.top());
	}

	@Test
	public void testAddMultiRemove() {
		for (int i = 0; i < 1000; i++) {
			queue.add(i);
		}
		queue.remove(0);
		Assertions.assertEquals(1, queue.top());
	}

	@Test
	public void testSort() {
		var sortedNumbers = new ArrayList<Integer>();
		for (int i = 0; i < 1000; i++) {
			sortedNumbers.add(i);
		}
		var shuffledNumbers = new ArrayList<>(sortedNumbers);
		Collections.shuffle(shuffledNumbers);
		for (Integer number : shuffledNumbers) {
			queue.add(number);
		}

		var newSortedNumbers = new ArrayList<>();
		Integer popped;
		while ((popped = queue.pop()) != null) {
			newSortedNumbers.add(popped);
		}

		Assertions.assertEquals(sortedNumbers, newSortedNumbers);
	}

	@AfterEach
	public void afterEach() throws IOException {
		queue.close();
		env.close();
	}
}
