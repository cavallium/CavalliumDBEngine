package it.cavallium.dbengine;

import static io.netty5.buffer.api.internal.Statics.allocatorClosedException;
import static io.netty5.buffer.api.internal.Statics.standardDrop;

import io.netty5.buffer.api.AllocationType;
import io.netty5.buffer.api.AllocatorControl;
import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.MemoryManager;
import io.netty5.buffer.api.StandardAllocationTypes;
import io.netty5.buffer.api.internal.Statics;
import io.netty5.buffer.api.pool.PooledBufferAllocator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Supplier;

public class TestAllocatorImpl implements BufferAllocator, AllocatorControl {

	private final TestMemoryManager manager;
	private final AllocationType allocationType = StandardAllocationTypes.ON_HEAP;
	private volatile boolean closed;

	private TestAllocatorImpl(TestMemoryManager testMemoryManager) {
		this.manager = testMemoryManager;
	}

	public static TestAllocatorImpl create() {
		return new TestAllocatorImpl(new TestMemoryManager(MemoryManager.instance()));
	}

	@Override
	public boolean isPooling() {
		return false;
	}

	@Override
	public AllocationType getAllocationType() {
		return allocationType;
	}

	@Override
	public Buffer allocate(int size) {
		if (closed) {
			throw allocatorClosedException();
		}
		Statics.assertValidBufferSize(size);
		return manager.allocateShared(this, size, Statics.standardDrop(manager), allocationType);
	}

	@Override
	public Supplier<Buffer> constBufferSupplier(byte[] bytes) {
		if (closed) {
			throw allocatorClosedException();
		}
		Buffer constantBuffer = manager.allocateShared(
				this, bytes.length, standardDrop(manager), allocationType);
		constantBuffer.writeBytes(bytes).makeReadOnly();
		return () -> manager.allocateConstChild(constantBuffer);
	}

	@Override
	public void close() {
		closed = true;
	}

	public long getActiveAllocations() {
		return this.manager.getActiveAllocations();
	}

	@Override
	public BufferAllocator getAllocator() {
		return this;
	}

	private static class TestMemoryManager implements MemoryManager {

		private final MemoryManager instance;
		private final LongAdder activeAllocations = new LongAdder();

		public TestMemoryManager(MemoryManager instance) {
			this.instance = instance;
		}

		@Override
		public Buffer allocateShared(AllocatorControl allocatorControl,
				long size,
				Function<Drop<Buffer>, Drop<Buffer>> dropDecorator,
				AllocationType allocationType) {
			return instance.allocateShared(allocatorControl, size, this::createDrop, allocationType);
		}

		@Override
		public Buffer allocateConstChild(Buffer readOnlyConstParent) {
			return instance.allocateConstChild(readOnlyConstParent);
		}

		@Override
		public Object unwrapRecoverableMemory(Buffer buf) {
			return instance.unwrapRecoverableMemory(buf);
		}

		@Override
		public Buffer recoverMemory(AllocatorControl allocatorControl, Object recoverableMemory, Drop<Buffer> drop) {
			return instance.recoverMemory(allocatorControl, recoverableMemory, drop);
		}

		@Override
		public Object sliceMemory(Object memory, int offset, int length) {
			return instance.sliceMemory(memory, offset, length);
		}

		@Override
		public String implementationName() {
			return instance.implementationName();
		}

		private Drop<Buffer> createDrop(Drop<Buffer> drop) {
			activeAllocations.increment();
			return new Drop<>() {
				@Override
				public void drop(Buffer obj) {
					activeAllocations.decrement();
					drop.drop(obj);
				}

				@Override
				public Drop<Buffer> fork() {
					return createDrop(drop.fork());
				}

				@Override
				public void attach(Buffer obj) {
					drop.attach(obj);
				}
			};
		}

		public long getActiveAllocations() {
			return activeAllocations.longValue();
		}
	}
}
