package it.cavallium.dbengine.database;

import lombok.Value;
import org.jetbrains.annotations.Nullable;

@Value(staticConstructor = "of")
public class Delta<T> {
	@Nullable T previous;
	@Nullable T current;
}
