package it.cavallium.dbengine.client.query;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonSyntaxException;
import it.cavallium.dbengine.client.query.current.CurrentVersion;
import it.cavallium.dbengine.client.query.current.data.IBasicType;
import it.cavallium.dbengine.client.query.current.data.IType;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

public class QueryGson {

	private static final HashMap<Class<? extends IType>, Set<Class<? extends IBasicType>>> implementationClassesSerializers = new HashMap<>();
	private static final JsonElement EMPTY_JSON_OBJECT = new JsonObject();

	static {
		for (var superTypeClass : CurrentVersion.getSuperTypeClasses()) {
			implementationClassesSerializers.put(superTypeClass, CurrentVersion.getSuperTypeSubtypesClasses(superTypeClass));
		}
	}

	public static GsonBuilder registerAdapters(GsonBuilder gsonBuilder) {
		implementationClassesSerializers.forEach((interfaceClass, implementationClasses) -> {
			gsonBuilder.registerTypeAdapter(interfaceClass, new DbClassesGenericSerializer<>(implementationClasses));
		});
		return gsonBuilder;
	}

	public static class DbClassesGenericSerializer<T extends IType> implements JsonSerializer<T>, JsonDeserializer<T> {

		private final BiMap<String, Class<? extends IBasicType>> subTypes;

		public DbClassesGenericSerializer(Set<Class<? extends IBasicType>> implementationClasses) {
			subTypes = HashBiMap.create(implementationClasses.size());
			for (Class<? extends IBasicType> implementationClass : implementationClasses) {
				var name = implementationClass.getSimpleName();
				this.subTypes.put(name, implementationClass);
			}
		}

		@Override
		public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context) {
			JsonObject result = new JsonObject();
			Class<?> type = src.getClass();
			if (!subTypes.inverse().containsKey(type)) {
				throw new JsonSyntaxException("Unknown element type: " + type.getCanonicalName());
			}
			result.add("type", new JsonPrimitive(subTypes.inverse().get(src.getClass())));
			if (Arrays
					.stream(src.getClass().getDeclaredFields())
					.anyMatch(field -> !Modifier.isStatic(field.getModifiers()) && !Modifier.isTransient(field.getModifiers()))) {
				result.add("properties", context.serialize(src, src.getClass()));
			}

			return result;
		}

		@Override
		public T deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
			JsonObject jsonObject = json.getAsJsonObject();
			String type = jsonObject.get("type").getAsString();
			JsonElement element;
			if (jsonObject.has("properties")) {
				element = jsonObject.get("properties");
			} else {
				element = EMPTY_JSON_OBJECT;
			}

			if (!subTypes.containsKey(type)) {
				throw new JsonParseException("Unknown element type: " + type);
			}
			return context.deserialize(element, subTypes.get(type));
		}
	}
}