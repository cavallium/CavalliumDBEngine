package it.cavallium.dbengine.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import com.squareup.moshi.JsonReader;
import com.squareup.moshi.JsonWriter;
import com.squareup.moshi.JsonDataException;
import com.squareup.moshi.Types;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.List;
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

	private Moshi generatedMoshi() {
		return new MoshiPolymorphic<Object>(true, MoshiPolymorphic.GetterStyle.FIELDS) {
			@Override
			@SuppressWarnings({"unchecked", "rawtypes"})
			protected Set<Class<Object>> getAbstractClasses() {
				return Set.of((Class) Shape.class);
			}

			@Override
			@SuppressWarnings({"unchecked", "rawtypes"})
			protected Set<Class<Object>> getConcreteClasses() {
				return Set.of((Class) GeneratedValue.class, (Class) LegacyValue.class, (Class) Container.class);
			}

			@Override
			protected boolean shouldIgnoreField(String fieldName) {
				return fieldName.startsWith("has");
			}

			@Override
			protected Method getFieldPresenceGetter(Field field) {
				if (field.getDeclaringClass() != GeneratedValue.class || field.getName().equals("required")) {
					return null;
				}
				try {
					return GeneratedValue.class.getMethod("has" + Character.toUpperCase(field.getName().charAt(0))
							+ field.getName().substring(1));
				} catch (NoSuchMethodException e) {
					throw new AssertionError(e);
				}
			}

			@Override
			protected Method getFactoryMethod(Class<?> type, List<Class<?>> parameters) throws NoSuchMethodException {
				return type == Container.class
						? type.getMethod("ofNullable", parameters.toArray(Class<?>[]::new))
						: super.getFactoryMethod(type, parameters);
			}

			@Override
			protected List<JsonAdapter.Factory> getExtraAdapterFactories() {
				return List.of((type, annotations, moshi) -> {
					if (!(type instanceof ParameterizedType parameterized)
							|| parameterized.getRawType() != Wrapper.class) {
						return null;
					}
					JsonAdapter<Object> inner = moshi.adapter(parameterized.getActualTypeArguments()[0]);
					return new JsonAdapter<Wrapper<?>>() {
						@Override
						public Wrapper<?> fromJson(JsonReader reader) throws IOException {
							return new Wrapper<>(inner.fromJson(reader));
						}

						@Override
						public void toJson(JsonWriter writer, Wrapper<?> value) throws IOException {
							inner.toJson(writer, value == null ? null : value.value());
						}
					};
				});
			}
		}.registerAdapters(new Moshi.Builder()).build();
	}

	@Test
	public void optionalPrimitivesPreservePresenceAndZeroValues() throws Exception {
		var adapter = generatedMoshi().adapter(GeneratedValue.class);
		for (String absent : List.of("{\"required\":4}", "{\"count\":null,\"enabled\":null,\"required\":4}")) {
			var decoded = adapter.fromJson(absent);
			assertThat(decoded.hasCount()).isFalse();
			assertThat(decoded.hasEnabled()).isFalse();
			assertThat(decoded.count).isZero();
			assertThat(decoded.enabled).isFalse();
			assertThat(adapter.toJson(decoded)).isEqualTo("{\"required\":4}");
			assertThat(adapter.serializeNulls().toJson(decoded))
					.isEqualTo("{\"count\":null,\"enabled\":null,\"required\":4}");
		}
		var present = GeneratedValue.of(true, 0, true, false, 4);
		assertThat(adapter.toJson(present)).isEqualTo("{\"count\":0,\"enabled\":false,\"required\":4}");
		assertThat(adapter.fromJson(adapter.toJson(present))).isEqualTo(present);
		for (String invalid : List.of("{}", "{\"required\":null}")) {
			assertThatThrownBy(() -> adapter.fromJson(invalid)).isInstanceOf(JsonDataException.class)
					.hasMessageContaining("Required primitive field \"required\" is missing or null");
		}
	}

	@Test
	public void adaptersRoundTripNullsAndGenericNestedPolymorphicValues() throws Exception {
		var moshi = generatedMoshi();
		var value = GeneratedValue.of(false, 0, true, false, 4);
		var adapter = moshi.adapter(Container.class);
		var container = Container.ofNullable(new Wrapper<>(value), Arrays.asList(value, null));
		var json = adapter.toJson(container);
		assertThat(json).isEqualTo("{\"wrapped\":{\"type\":\"GeneratedValue\",\"properties\":{\"enabled\":false,\"required\":4}},"
				+ "\"values\":[{\"type\":\"GeneratedValue\",\"properties\":{\"enabled\":false,\"required\":4}},null]}");
		assertThat(adapter.fromJson(json)).isEqualTo(container);
		assertThat(adapter.fromJson("{}").wrapped).isEqualTo(new Wrapper<>(null));
		assertThat(adapter.fromJson("{\"wrapped\":null,\"values\":null}").wrapped).isEqualTo(new Wrapper<>(null));
		JsonAdapter<Wrapper<Shape>> wrapper = moshi.adapter(Types.newParameterizedTypeWithOwner(MoshiPolymorphicTest.class, Wrapper.class, Shape.class));
		assertThat(wrapper.fromJson(wrapper.toJson(new Wrapper<>(value)))).isEqualTo(new Wrapper<>(value));
		for (var type : List.of(Shape.class, GeneratedValue.class,
				Types.newParameterizedType(List.class, Shape.class),
				Types.newParameterizedType(List.class, GeneratedValue.class))) {
			var nullable = moshi.adapter(type);
			assertThat(nullable.toJson(null)).isEqualTo("null");
			assertThat(nullable.fromJson("null")).isNull();
		}
	}

	@Test
	public void legacyRecordKeepsItsWireShape() throws Exception {
		var moshi = generatedMoshi();
		var value = LegacyValue.of(7, "legacy");
		var adapter = moshi.adapter(Shape.class);
		String json = "{\"type\":\"LegacyValue\",\"properties\":{\"id\":7,\"name\":\"legacy\"}}";
		assertThat(adapter.toJson(value)).isEqualTo(json);
		assertThat(adapter.fromJson(json)).isEqualTo(value);
	}

	public interface Shape {}

	public record Wrapper<T>(T value) {}

	public record LegacyValue(int id, String name) implements Shape {
		public static LegacyValue of(int id, String name) { return new LegacyValue(id, name); }
	}

	public record Container(Wrapper<Shape> wrapped, List<Shape> values) {
		public static Container ofNullable(Wrapper<Shape> wrapped, List<Shape> values) {
			return new Container(wrapped == null ? new Wrapper<>(null) : wrapped, values);
		}
	}

	public static final class GeneratedValue implements Shape {
		private final int count;
		private final boolean enabled;
		private final int required;
		private final boolean hasCount;
		private final boolean hasEnabled;

		private GeneratedValue(boolean hasCount, int count, boolean hasEnabled, boolean enabled, int required) {
			this.hasCount = hasCount;
			this.count = count;
			this.hasEnabled = hasEnabled;
			this.enabled = enabled;
			this.required = required;
		}

		public static GeneratedValue of(boolean hasCount, int count, boolean hasEnabled, boolean enabled, int required) {
			return new GeneratedValue(hasCount, count, hasEnabled, enabled, required);
		}
		public boolean hasCount() { return hasCount; }
		public boolean hasEnabled() { return hasEnabled; }
		@Override
		public boolean equals(Object obj) {
			return obj instanceof GeneratedValue other && count == other.count && enabled == other.enabled
					&& required == other.required && hasCount == other.hasCount && hasEnabled == other.hasEnabled;
		}
		@Override
		public int hashCode() { return java.util.Objects.hash(count, enabled, required, hasCount, hasEnabled); }
	}

	@Test
	public void mutableFieldsKeepDefaultsWhenOmittedAndRejectExplicitPrimitiveNull() throws Exception {
		var adapter = new MoshiPolymorphic<Object>() {
			@Override
			protected Set<Class<Object>> getAbstractClasses() { return Set.of(); }

			@Override
			@SuppressWarnings({"unchecked", "rawtypes"})
			protected Set<Class<Object>> getConcreteClasses() { return Set.of((Class) MutableValue.class); }

			@Override
			protected boolean shouldIgnoreField(String fieldName) { return false; }
		}.registerAdapters(new Moshi.Builder()).build().adapter(MutableValue.class);

		var decoded = adapter.fromJson("{}");
		assertThat(decoded.count).isEqualTo(7);
		assertThat(decoded.name).isEqualTo("default");
		assertThat(adapter.fromJson("{\"count\":0,\"name\":null}").count).isZero();
		assertThat(adapter.fromJson("{\"name\":null}").name).isNull();
		assertThatThrownBy(() -> adapter.fromJson("{\"count\":null}"))
				.isInstanceOf(JsonDataException.class).hasMessageContaining("Required primitive field \"count\"");
	}

	@Test
	public void presenceGettersRequireStaticFactoryInstantiation() {
		var polymorphic = new MoshiPolymorphic<Object>() {
			@Override
			protected Set<Class<Object>> getAbstractClasses() { return Set.of(); }

			@Override
			@SuppressWarnings({"unchecked", "rawtypes"})
			protected Set<Class<Object>> getConcreteClasses() { return Set.of((Class) GeneratedValue.class); }

			@Override
			protected boolean shouldIgnoreField(String fieldName) { return fieldName.startsWith("has"); }

			@Override
			protected Method getFieldPresenceGetter(Field field) {
				if (!field.getName().equals("count")) {
					return null;
				}
				try {
					return GeneratedValue.class.getMethod("hasCount");
				} catch (NoSuchMethodException e) {
					throw new AssertionError(e);
				}
			}
		};
		assertThatThrownBy(() -> polymorphic.registerAdapters(new Moshi.Builder()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Presence getters require static factory instantiation");
	}

	public static final class MutableValue {
		public int count = 7;
		public String name = "default";
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
