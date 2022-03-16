package it.cavallium.dbengine.database.disk;

import io.netty5.util.internal.PlatformDependent;
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
		Lookup lookup = MethodHandles.lookup();

		Object nativeVal = null;

		var ofNativeRestricted = getJava16NativeRestricted(lookup);
		if (ofNativeRestricted == null) {
			cause = null;
			ofNativeRestricted = getJava17NativeRestricted(lookup);
		}
		if (ofNativeRestricted != null) {
			try {
				nativeVal = ofNativeRestricted.invoke();
			} catch (Throwable e) {
				cause = e;
			}
		}
		OF_NATIVE_RESTRICTED = ofNativeRestricted;

		MethodHandle asSlice;
		try {
			asSlice = lookup.findVirtual(lookup.findClass("jdk.incubator.foreign.MemorySegment"),
					"asSlice",
					MethodType.methodType(lookup.findClass("jdk.incubator.foreign.MemorySegment"), long.class, long.class)
			);
		} catch (NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
			asSlice = null;
			cause = e;
		}
		AS_SLICE = asSlice;

		MethodHandle asByteBuffer;
		try {
			asByteBuffer = lookup.findVirtual(lookup.findClass("jdk.incubator.foreign.MemorySegment"),
					"asByteBuffer", MethodType.methodType(ByteBuffer.class));
		} catch (NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
			asByteBuffer = null;
			cause = e;
		}
		AS_BYTE_BUFFER = asByteBuffer;

		NATIVE = nativeVal;
	}

	@SuppressWarnings("JavaLangInvokeHandleSignature")
	private static MethodHandle getJava16NativeRestricted(Lookup lookup) {
		MethodHandle ofNativeRestricted;
		try {
			ofNativeRestricted = lookup.findStatic(lookup.findClass("jdk.incubator.foreign.MemorySegment"),
					"ofNativeRestricted",
					MethodType.methodType(lookup.findClass("jdk.incubator.foreign.MemorySegment"))
			);
		} catch (NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
			ofNativeRestricted = null;
			cause = e;
		}
		return ofNativeRestricted;
	}

	@SuppressWarnings("JavaLangInvokeHandleSignature")
	private static MethodHandle getJava17NativeRestricted(Lookup lookup) {
		MethodHandle ofNativeRestricted;
		try {
			ofNativeRestricted = lookup.findStatic(lookup.findClass("jdk.incubator.foreign.MemorySegment"),
					"globalNativeSegment",
					MethodType.methodType(lookup.findClass("jdk.incubator.foreign.MemorySegment"))
			);
		} catch (NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
			ofNativeRestricted = null;
			cause = e;
		}
		return ofNativeRestricted;
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
					return PlatformDependent.directBuffer(address, (int) size, null);
				}
				throw new UnsupportedOperationException("Foreign Memory Access API is disabled!"
						+ " Please set \"" + MemorySegmentUtils.getSuggestedArgs() + "\"",
						getUnsupportedCause()
				);
			}
			var memorySegment = AS_SLICE.invoke(NATIVE, address, size);
			return (ByteBuffer) AS_BYTE_BUFFER.invoke(memorySegment);
		} catch (Throwable e) {
			throw new UnsupportedOperationException("Foreign Memory Access API is disabled!"
					+ " Please set \"" + MemorySegmentUtils.getSuggestedArgs() + "\"", e);
		}
	}

	public static boolean isSupported() {
		return OF_NATIVE_RESTRICTED != null && AS_SLICE != null && AS_BYTE_BUFFER != null && NATIVE != null;
	}

	public static Throwable getUnsupportedCause() {
		return cause;
	}

	public static String getSuggestedArgs() {
		return "--enable-preview --add-modules jdk.incubator.foreign -Dforeign.restricted=permit --enable-native-access=ALL-UNNAMED";
	}
}
