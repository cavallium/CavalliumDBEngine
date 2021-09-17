package it.cavallium.dbengine.database.disk;

import io.net5.util.internal.PlatformDependent;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;

public class MemorySegmentUtils {

	private static final MethodHandle OF_NATIVE_RESTRICTED;
	private static final MethodHandle AS_SLICE;
	private static final MethodHandle AS_BYTE_BUFFER;
	private static Throwable cause;

	private static final Object NATIVE;

	static {
		Lookup lookup = MethodHandles.publicLookup();

		Object nativeVal = null;

		MethodHandle ofNativeRestricted;
		try {
			ofNativeRestricted = lookup.findStatic(Class.forName("jdk.incubator.foreign.MemorySegment"),
					"ofNativeRestricted",
					MethodType.methodType(Class.forName("jdk.incubator.foreign.MemorySegment"))
			);
			try {
				nativeVal = ofNativeRestricted.invoke();
			} catch (Throwable e) {
				cause = e;
			}
		} catch (NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
			ofNativeRestricted = null;
			cause = e;
		}
		OF_NATIVE_RESTRICTED = ofNativeRestricted;

		MethodHandle asSlice;
		try {
			asSlice = lookup.findVirtual(Class.forName("jdk.incubator.foreign.MemorySegment"),
					"asSlice",
					MethodType.methodType(Class.forName("jdk.incubator.foreign.MemorySegment"), long.class, long.class)
			);
		} catch (NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
			asSlice = null;
			cause = e;
		}
		AS_SLICE = asSlice;

		MethodHandle asByteBuffer;
		try {
			asByteBuffer = lookup.findVirtual(Class.forName("jdk.incubator.foreign.MemorySegment"),
					"asByteBuffer", MethodType.methodType(ByteBuffer.class));
		} catch (NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
			asByteBuffer = null;
			cause = e;
		}
		AS_BYTE_BUFFER = asByteBuffer;

		NATIVE = nativeVal;
	}

	public static ByteBuffer directBuffer(long address, long size) {
		if (address <= 0) {
			throw new IllegalArgumentException("Address is " + address);
		}
		if (size > Integer.MAX_VALUE || size < 0) {
			throw new IllegalArgumentException("size is " + size);
		}
		try {
			if (!isSupported()) {
				if (PlatformDependent.hasDirectBufferNoCleanerConstructor()) {
					return PlatformDependent.directBuffer(address, (int) size);
				}
				throw new UnsupportedOperationException("Foreign Memory Access API is disabled!"
						+ " Please set \"--enable-preview --add-modules jdk.incubator.foreign -Dforeign.restricted=permit\"");
			}
			var memorySegment = AS_SLICE.invoke(NATIVE, address, size);
			return (ByteBuffer) AS_BYTE_BUFFER.invoke(memorySegment);
		} catch (Throwable e) {
			throw new UnsupportedOperationException("Foreign Memory Access API is disabled!"
					+ " Please set \"--enable-preview --add-modules jdk.incubator.foreign -Dforeign.restricted=permit\"", e);
		}
	}

	public static boolean isSupported() {
		return OF_NATIVE_RESTRICTED != null && AS_SLICE != null && AS_BYTE_BUFFER != null && NATIVE != null;
	}

	public static Throwable getUnsupportedCause() {
		return cause;
	}
}
