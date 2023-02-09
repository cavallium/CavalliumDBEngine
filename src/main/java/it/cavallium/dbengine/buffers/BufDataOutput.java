package it.cavallium.dbengine.buffers;

import it.unimi.dsi.fastutil.Arrays;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.warp.commonutils.stream.SafeByteArrayOutputStream;
import org.warp.commonutils.stream.SafeDataOutputStream;

public class BufDataOutput implements DataOutput {

	private final SafeByteArrayOutputStream buf;
	private final SafeDataOutputStream dOut;
	private final int limit;

	private BufDataOutput(SafeByteArrayOutputStream buf) {
		this.buf = buf;
		this.dOut = new SafeDataOutputStream(buf);
		limit = Integer.MAX_VALUE;
	}

	private BufDataOutput(SafeByteArrayOutputStream buf, int maxSize) {
		this.buf = buf;
		this.dOut = new SafeDataOutputStream(buf);
		this.limit = maxSize;
	}

	public static BufDataOutput createLimited(int maxSize, int hint) {
		if (hint >= 0) {
			if (maxSize < 0 || maxSize == Integer.MAX_VALUE) {
				return create(hint);
			} else {
				return new BufDataOutput(new SafeByteArrayOutputStream(Math.min(maxSize, hint)), maxSize);
			}
		} else {
			return createLimited(maxSize);
		}
	}

	public static BufDataOutput createLimited(int maxSize) {
		if (maxSize < 0 || maxSize == Integer.MAX_VALUE) {
			return create();
		} else {
			return new BufDataOutput(new SafeByteArrayOutputStream(maxSize), maxSize);
		}
	}

	public static BufDataOutput create() {
		return new BufDataOutput(new SafeByteArrayOutputStream());
	}

	public static BufDataOutput create(int hint) {
		if (hint >= 0) {
			return new BufDataOutput(new SafeByteArrayOutputStream(hint));
		} else {
			return create();
		}
	}

	public static BufDataOutput wrap(Buf buf, int from, int to) {
		Arrays.ensureFromTo(buf.size(), from, to);
		if (buf.isEmpty()) {
			return createLimited(0);
		} else {
			return new BufDataOutput(buf.binaryOutputStream(from), to - from);
		}
	}

	public static BufDataOutput wrap(Buf buf) {
		if (buf.isEmpty()) {
			return createLimited(0);
		} else {
			return new BufDataOutput(buf.binaryOutputStream(), buf.size());
		}
	}

	private IllegalStateException unreachable(IOException ex) {
		return new IllegalStateException(ex);
	}

	@Override
	public void write(int b) {
		checkOutOfBounds(1);
		dOut.write(b);
	}

	private void checkOutOfBounds(int delta) {
		if (dOut.size() + delta > limit) {
			throw new IndexOutOfBoundsException(limit);
		}
	}

	@Override
	public void write(byte @NotNull [] b) {
		checkOutOfBounds(b.length);
		dOut.write(b);
	}

	@Override
	public void write(byte @NotNull [] b, int off, int len) {
		checkOutOfBounds(Math.max(0, Math.min(b.length - off, len)));
		dOut.write(b, off, len);
	}

	@Override
	public void writeBoolean(boolean v) {
		checkOutOfBounds(1);
		dOut.writeBoolean(v);
	}

	@Override
	public void writeByte(int v) {
		checkOutOfBounds(Byte.BYTES);
		dOut.writeByte(v);
	}

	@Override
	public void writeShort(int v) {
		checkOutOfBounds(Short.BYTES);
		dOut.writeShort(v);
	}

	@Override
	public void writeChar(int v) {
		checkOutOfBounds(Character.BYTES);
		dOut.writeChar(v);
	}

	@Override
	public void writeInt(int v) {
		checkOutOfBounds(Integer.BYTES);
		dOut.writeInt(v);
	}

	@Override
	public void writeLong(long v) {
		checkOutOfBounds(Long.BYTES);
		dOut.writeLong(v);
	}

	@Override
	public void writeFloat(float v) {
		checkOutOfBounds(Float.BYTES);
		dOut.writeFloat(v);
	}

	@Override
	public void writeDouble(double v) {
		checkOutOfBounds(Double.BYTES);
		dOut.writeDouble(v);
	}

	public void ensureWritable(int size) {
		dOut.flush();
		buf.ensureWritable(size);
	}

	@Override
	public void writeBytes(@NotNull String s) {
		checkOutOfBounds(s.length() * Byte.BYTES);
		dOut.writeBytes(s);
	}

	// todo: check
	public void writeBytes(Buf deserialized) {
		checkOutOfBounds(deserialized.size());
		deserialized.writeTo(dOut);
	}

	public void writeBytes(byte[] b, int off, int len) {
		write(b, off, len);
	}

	@Override
	public void writeChars(@NotNull String s) {
		checkOutOfBounds(Character.BYTES * s.length());
		dOut.writeChars(s);
	}

	@Override
	public void writeUTF(@NotNull String s) {
		throw new UnsupportedOperationException();
	}

	public Buf asList() {
		dOut.flush();
		return Buf.wrap(this.buf.array, this.buf.length);
	}

	@Override
	public String toString() {
		return dOut.toString();
	}

	@Override
	public int hashCode() {
		return dOut.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		BufDataOutput that = (BufDataOutput) o;

		return Objects.equals(dOut, that.dOut);
	}

	public int size() {
		return dOut.size();
	}
}
