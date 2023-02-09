package it.cavallium.dbengine.buffers;

import org.jetbrains.annotations.NotNull;
import org.warp.commonutils.stream.SafeByteArrayInputStream;
import org.warp.commonutils.stream.SafeDataInputStream;


public class BufDataInput extends SafeDataInputStream {

	/**
	 * Creates a DataInputStream that uses the specified underlying InputStream.
	 *
	 * @param in the specified input stream
	 */
	private BufDataInput(@NotNull SafeByteArrayInputStream in) {
		super(in);
	}

	public static BufDataInput create(Buf byteList) {
		return new BufDataInput(byteList.binaryInputStream());
	}

	@Deprecated
	@Override
	public void close() {
	}

	@Override
	public void mark(int readlimit) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void reset() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean markSupported() {
		return false;
	}
}
