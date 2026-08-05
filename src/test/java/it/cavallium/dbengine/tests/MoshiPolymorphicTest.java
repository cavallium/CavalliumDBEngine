package it.cavallium.dbengine.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import it.cavallium.dbengine.utils.MoshiPolymorphic;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class MoshiPolymorphicTest {

	@Test
	public void fieldsGetterStyleSupportsPrivateImmutableFields() throws Exception {
		var polymorphic = new MoshiPolymorphic<Object>(true, MoshiPolymorphic.GetterStyle.FIELDS) {
			@Override
			protected Set<Class<Object>> getAbstractClasses() {
				return Set.of();
			}

			@Override
			@SuppressWarnings({"unchecked", "rawtypes"})
			protected Set<Class<Object>> getConcreteClasses() {
				return Set.of((Class) OptimizedValue.class);
			}

			@Override
			protected Map<Class<?>, JsonAdapter<?>> getExtraAdapters() {
				return Map.of();
			}

			@Override
			protected boolean shouldIgnoreField(String fieldName) {
				return false;
			}
		};
		var moshiBuilder = polymorphic.registerAdapters(new Moshi.Builder());
		var adapter = moshiBuilder.build().adapter(OptimizedValue.class);
		var value = OptimizedValue.of(new byte[]{1, 2, 3}, null);

		var json = adapter.toJson(value);
		var decoded = adapter.fromJson(json);

		assertThat(json).isEqualTo("{\"data\":[1,2,3]}");
		assertThat(decoded).isEqualTo(value);
	}

	public static final class OptimizedValue {

		private final byte[] data;
		private final String optional;

		private OptimizedValue(byte[] data, String optional) {
			this.data = data;
			this.optional = optional;
		}

		public static OptimizedValue of(byte[] data, String optional) {
			return new OptimizedValue(data.clone(), optional);
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof OptimizedValue other
					&& java.util.Arrays.equals(data, other.data)
					&& java.util.Objects.equals(optional, other.optional);
		}

		@Override
		public int hashCode() {
			return 31 * java.util.Arrays.hashCode(data) + java.util.Objects.hashCode(optional);
		}
	}
}
