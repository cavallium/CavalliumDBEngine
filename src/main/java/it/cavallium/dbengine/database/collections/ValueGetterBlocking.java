package it.cavallium.dbengine.database.collections;

import java.io.IOException;

public interface ValueGetterBlocking<KEY, VALUE> {

	VALUE get(KEY key) throws IOException;
}
