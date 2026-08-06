package it.cavallium.dbengine.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.serialization.DatabaseDataInput;
import org.junit.jupiter.api.Test;

class DatabaseDataInputTest {

	@Test
	void opensStoredBuffersThroughTheExplicitLimitsApi() {
		var input = DatabaseDataInput.create(Buf.wrap((byte) 7, (byte) 11));

		assertEquals(7, input.readUnsignedByte());
		assertEquals(11, input.readUnsignedByte());
		assertEquals(0, input.available());
	}
}
