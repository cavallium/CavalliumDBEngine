package it.cavallium.dbengine.database.serialization;

import it.cavallium.buffer.Buf;
import it.cavallium.buffer.BufDataInput;
import it.cavallium.datagen.DecodeLimits;
import java.util.Objects;

/** Creates inputs for serialized values already stored by the database. */
public final class DatabaseDataInput {

	private static final DecodeLimits TRUSTED_STORAGE_LIMITS = DecodeLimits.unlimited();

	private DatabaseDataInput() {}

	/**
	 * Opens a complete value read from trusted database storage.
	 *
	 * <p>The unlimited policy is explicit and centralized here so external or partially trusted
	 * inputs cannot accidentally inherit it.</p>
	 */
	public static BufDataInput create(Buf serializedValue) {
		return BufDataInput.create(Objects.requireNonNull(serializedValue, "serializedValue"),
				TRUSTED_STORAGE_LIMITS);
	}
}
