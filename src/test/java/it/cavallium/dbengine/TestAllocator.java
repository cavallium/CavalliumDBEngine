package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.destroyAllocator;
import static it.cavallium.dbengine.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.DbTestUtils.newAllocator;

import io.netty5.buffer.Buffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAllocator {


	private DbTestUtils.TestAllocator allocator;

	@BeforeEach
	public void beforeEach() {
		this.allocator = newAllocator();
		ensureNoLeaks(allocator.allocator(), false, false);
	}

	@AfterEach
	public void afterEach() {
		ensureNoLeaks(allocator.allocator(), true, false);
		destroyAllocator(allocator);
	}

	@Test
	public void testNoOp() {
	}

	@Test
	public void testShouldPass() {
		Buffer allocated = allocator.allocator().allocate(5000);
		allocated.close();
		ensureNoLeaks(allocator.allocator(), true, false);
	}

	@Test
	public void testShouldFail() {
		Buffer allocated = null;
		try {
			boolean failed;
			try {
				allocated = allocator.allocator().allocate(5000);
				ensureNoLeaks(allocator.allocator(), true, true);
				failed = false;
			} catch (Exception ex) {
				failed = true;
			}
			if (!failed) {
				Assertions.fail("A leak was not detected!");
			}
		} finally {
			if (allocated != null) {
				allocated.close();
			}
		}
	}
}
