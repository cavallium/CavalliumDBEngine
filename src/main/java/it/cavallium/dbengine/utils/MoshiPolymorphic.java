package it.cavallium.dbengine.utils;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.JsonDataException;
import com.squareup.moshi.JsonReader;
import com.squareup.moshi.JsonReader.Options;
import com.squareup.moshi.JsonWriter;
import com.squareup.moshi.Moshi;
import com.squareup.moshi.Types;
import dev.zacsweers.moshix.records.RecordsJsonAdapterFactory;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class MoshiPolymorphic<OBJ> {

	public enum GetterStyle {
		FIELDS,
		RECORDS_GETTERS,
		STANDARD_GETTERS
	}

	private final boolean instantiateUsingStaticOf;
	private final GetterStyle getterStyle;
	private boolean initialized = false;
	private Moshi abstractMoshi;
	private final Map<Type, JsonAdapter<OBJ>> abstractClassesSerializers = new ConcurrentHashMap<>();
	private final Map<Type, JsonAdapter<List<OBJ>>> abstractListClassesSerializers = new ConcurrentHashMap<>();
	private final Map<Type, JsonAdapter<OBJ>> concreteClassesSerializers = new ConcurrentHashMap<>();
	private final Map<Type, JsonAdapter<List<OBJ>>> concreteListClassesSerializers = new ConcurrentHashMap<>();
	private final Map<Type, JsonAdapter<?>> extraClassesSerializers = new ConcurrentHashMap<>();
	private final Map<String, JsonAdapter<OBJ>> customAdapters = new ConcurrentHashMap<>();

	public MoshiPolymorphic() {
		this(false, GetterStyle.FIELDS);
	}

	public MoshiPolymorphic(boolean instantiateUsingStaticOf, GetterStyle getterStyle) {
		this.instantiateUsingStaticOf = instantiateUsingStaticOf;
		this.getterStyle = getterStyle;
	}

	private synchronized void initialize() {
		if (!this.initialized) {
			this.initialized = true;
			var abstractMoshiBuilder = new Moshi.Builder();
			var abstractClasses = getAbstractClasses();
			var concreteClasses = getConcreteClasses();
			var extraAdapters = getExtraAdapters();

			extraAdapters.forEach((extraClass, jsonAdapter) -> {
				extraClassesSerializers.put(extraClass, jsonAdapter);
				abstractMoshiBuilder.add(extraClass, jsonAdapter);
			});

			for (Class<?> declaredClass : abstractClasses) {
				var name = fixType(declaredClass.getSimpleName());
				JsonAdapter<OBJ> adapter = new PolymorphicAdapter<>(name);
				if (!extraClassesSerializers.containsKey(declaredClass)) {
					abstractMoshiBuilder.add(declaredClass, adapter);
					abstractClassesSerializers.put(declaredClass, adapter);
					abstractListClassesSerializers.put(Types.newParameterizedType(List.class, declaredClass),
							new ListValueAdapter<>(adapter)
					);
				}
				customAdapters.put(name, adapter);
			}

			for (Class<?> declaredClass : concreteClasses) {
				var name = fixType(declaredClass.getSimpleName());
				JsonAdapter<OBJ> adapter = new NormalValueAdapter<>(name, declaredClass);
				if (!extraClassesSerializers.containsKey(declaredClass)
						&& !abstractClassesSerializers.containsKey(declaredClass)) {
					concreteClassesSerializers.put(declaredClass, adapter);
					concreteListClassesSerializers.put(Types.newParameterizedType(List.class, declaredClass),
							new ListValueAdapter<>(adapter)
					);
					abstractMoshiBuilder.add(declaredClass, adapter);
				}
				customAdapters.put(name, adapter);
			}

			abstractMoshiBuilder.addLast(new RecordsJsonAdapterFactory());

			abstractMoshi = abstractMoshiBuilder.build();
		}
	}

	protected abstract Set<Class<OBJ>> getAbstractClasses();

	protected abstract Set<Class<OBJ>> getConcreteClasses();

	protected Map<Class<?>, JsonAdapter<?>> getExtraAdapters() {
		return Map.of();
	}

	protected abstract boolean shouldIgnoreField(String fieldName);

	public Moshi.Builder registerAdapters(Moshi.Builder moshiBuilder) {
		initialize();
		extraClassesSerializers.forEach(moshiBuilder::add);
		abstractClassesSerializers.forEach(moshiBuilder::add);
		abstractListClassesSerializers.forEach(moshiBuilder::add);
		concreteClassesSerializers.forEach(moshiBuilder::add);
		concreteListClassesSerializers.forEach(moshiBuilder::add);
		return moshiBuilder;
	}

	private class PolymorphicAdapter<T> extends JsonAdapter<T> {

		private final String adapterName;

		private PolymorphicAdapter(String adapterName) {
			this.adapterName = adapterName;
		}

		private final Options NAMES = Options.of("type", "properties");

		@Nullable
		@Override
		public T fromJson(@NotNull JsonReader jsonReader) {
			String type = null;

			jsonReader.beginObject();
			iterate: while (jsonReader.hasNext()) {
				switch (jsonReader.selectName(NAMES)) {
					case 0:
						type = fixType(jsonReader.nextString());
						break;
					case 1:
						if (type == null) {
							throw new JsonDataException("Type must be defined before properties");
						}
						break iterate;
					default:
						String name = jsonReader.nextName();
						throw new JsonDataException("Key \"" + name + "\" is invalid");
				}
			}

			JsonAdapter<? extends OBJ> propertiesAdapter = customAdapters.get(type);
			if (propertiesAdapter == null) {
				throw new JsonDataException("Type \"" + type + "\" is unknown");
			}
			//noinspection unchecked
			var result = (T) propertiesAdapter.fromJson(jsonReader);

			jsonReader.endObject();

			return result;
		}

		@Override
		public void toJson(@NotNull JsonWriter jsonWriter, @Nullable T t) {
			if (t == null) {
				jsonWriter.nullValue();
			} else {
				String type = fixType(t.getClass().getSimpleName());

				JsonAdapter<OBJ> propertiesAdapter = customAdapters.get(type);
				if (propertiesAdapter == null) {
					abstractMoshi.adapter(Object.class).toJson(jsonWriter, t);
				} else {
					jsonWriter.beginObject();
					jsonWriter.name("type").value(type);
					jsonWriter.name("properties");
					//noinspection unchecked
					propertiesAdapter.toJson(jsonWriter, (OBJ) t);
					jsonWriter.endObject();
				}
			}
		}
	}

	private class NormalValueAdapter<T> extends JsonAdapter<T> {

		private final String adapterName;
		private final Options names;
		private final Class<?> declaredClass;
		private final List<Field> declaredFields;
		private final Function<T, Object>[] fieldGetters;

		private NormalValueAdapter(String adapterName, Class<?> declaredClass) {
			try {
				this.adapterName = adapterName;
				this.declaredClass = declaredClass;
				this.declaredFields = Arrays
						.stream(declaredClass.getDeclaredFields())
						.filter(field -> {
							var modifiers = field.getModifiers();
							return !Modifier.isStatic(modifiers)
									&& !Modifier.isTransient(modifiers)
									&& !shouldIgnoreField(field.getName());
						})
						.collect(Collectors.toList());
				String[] fieldNames = new String[this.declaredFields.size()];
				//noinspection unchecked
				this.fieldGetters = new Function[this.declaredFields.size()];
				int i = 0;
				for (Field declaredField : this.declaredFields) {
					fieldNames[i] = declaredField.getName();

					switch (getterStyle) {
						case STANDARD_GETTERS:
							var getterMethod = declaredField
									.getDeclaringClass()
									.getMethod("get" + StringUtils.capitalize(declaredField.getName()));
							fieldGetters[i] = obj -> {
								try {
									return getterMethod.invoke(obj);
								} catch (InvocationTargetException | IllegalAccessException e) {
									throw new RuntimeException(e);
								}
							};
							break;
						case RECORDS_GETTERS:
							var getterMethod2 = declaredField
									.getDeclaringClass()
									.getMethod(declaredField.getName());
							fieldGetters[i] = obj -> {
								try {
									return getterMethod2.invoke(obj);
								} catch (InvocationTargetException | IllegalAccessException e) {
									throw new RuntimeException(e);
								}
							};
							break;
						case FIELDS:
							fieldGetters[i] = t -> {
								try {
									return declaredField.get(t);
								} catch (IllegalAccessException e) {
									throw new RuntimeException(e);
								}
							};
							break;
					}

					i++;
				} this.names = Options.of(fieldNames);
			} catch (NoSuchMethodException e) {
				throw new RuntimeException(e);
			}
		}

		@Nullable
		@Override
		public T fromJson(@NotNull JsonReader jsonReader) {
			try {
				Object instance;
				Object[] fields;
				if (instantiateUsingStaticOf) {
					fields = new Object[declaredFields.size()];
					instance = null;
				} else {
					fields = null;
					instance = declaredClass.getConstructor().newInstance();
				}

				jsonReader.beginObject();
				while (jsonReader.hasNext()) {
					var nameId = jsonReader.selectName(names);
					if (nameId >= 0 && nameId < this.declaredFields.size()) {
						var fieldValue = abstractMoshi.adapter(declaredFields.get(nameId).getGenericType()).fromJson(jsonReader);
						if (instantiateUsingStaticOf) {
							fields[nameId] = fieldValue;
						} else {
							declaredFields.get(nameId).set(instance, fieldValue);
						}
					} else {
						String keyName = jsonReader.nextName();
						throw new JsonDataException("Key \"" + keyName + "\" is invalid");
					}
				}
				jsonReader.endObject();

				if (instantiateUsingStaticOf) {
					Class[] params = new Class[declaredFields.size()];
					for (int i = 0; i < declaredFields.size(); i++) {
						params[i] = declaredFields.get(i).getType();
					}
					instance = declaredClass.getMethod("of", params).invoke(null, fields);
				}

				//noinspection unchecked
				return (T) instance;
			} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				throw new JsonDataException(e);
			}
		}

		@Override
		public void toJson(@NotNull JsonWriter jsonWriter, @Nullable T t) {
			if (t == null) {
				jsonWriter.nullValue();
			} else {
				jsonWriter.beginObject();
				int i = 0;
				for (Field declaredField : declaredFields) {
					jsonWriter.name(declaredField.getName());
					Class<?> fieldType = declaredField.getType();
					if (abstractClassesSerializers.containsKey(fieldType)) {
						//noinspection unchecked
						abstractClassesSerializers.<OBJ>get(fieldType).toJson(jsonWriter, (OBJ) fieldGetters[i].apply(t));
					} else if (concreteClassesSerializers.containsKey(fieldType)) {
						//noinspection unchecked
						concreteClassesSerializers.<OBJ>get(fieldType).toJson(jsonWriter, (OBJ) fieldGetters[i].apply(t));
					} else {
						abstractMoshi.<Object>adapter(fieldType).toJson(jsonWriter, fieldGetters[i].apply(t));
					}
					i++;
				}
				jsonWriter.endObject();
			}
		}
	}

	private static class ListValueAdapter<T> extends JsonAdapter<List<T>> {

		private final JsonAdapter<T> valueAdapter;

		public ListValueAdapter(JsonAdapter<T> valueAdapter) {
			this.valueAdapter = valueAdapter;
		}

		@Nullable
		@Override
		public List<T> fromJson(@NotNull JsonReader jsonReader) {
			jsonReader.beginArray();
			var result = new ArrayList<T>();
			while (jsonReader.hasNext()) {
				result.add(valueAdapter.fromJson(jsonReader));
			}
			jsonReader.endArray();
			return Collections.unmodifiableList(result);
		}

		@Override
		public void toJson(@NotNull JsonWriter jsonWriter, @Nullable List<T> ts) {
			if (ts == null) {
				jsonWriter.nullValue();
			} else {
				jsonWriter.beginArray();
				for (T value : ts) {
					valueAdapter.toJson(jsonWriter.valueSink(), value);
				}
				jsonWriter.endArray();
			}
		}
	}

	private static String fixType(String nextString) {
		if (nextString.length() > 512) {
			throw new IllegalArgumentException("Input too long: " + nextString.length());
		}
		return UTFUtils.keepOnlyASCII(nextString);
	}
}