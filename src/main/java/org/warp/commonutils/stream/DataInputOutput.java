package org.warp.commonutils.stream;

import java.io.DataInput;
import java.io.DataOutput;

public interface DataInputOutput extends DataInput, DataOutput {

	DataInput getIn();

	DataOutput getOut();
}
