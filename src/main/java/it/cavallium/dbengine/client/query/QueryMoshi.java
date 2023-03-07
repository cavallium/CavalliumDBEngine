package it.cavallium.dbengine.client.query;

import com.squareup.moshi.JsonAdapter;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.IntOpenHashSetJsonAdapter;
import it.cavallium.dbengine.client.query.current.CurrentVersion;
import it.cavallium.dbengine.client.query.current.IBaseType;
import it.cavallium.dbengine.client.query.current.IType;
import it.cavallium.dbengine.utils.BooleanListJsonAdapter;
import it.cavallium.dbengine.utils.BufJsonAdapter;
import it.cavallium.dbengine.utils.ByteListJsonAdapter;
import it.cavallium.dbengine.utils.CharListJsonAdapter;
import it.cavallium.dbengine.utils.IntListJsonAdapter;
import it.cavallium.dbengine.utils.LongListJsonAdapter;
import it.cavallium.dbengine.utils.MoshiPolymorphic;
import it.cavallium.dbengine.utils.ShortListJsonAdapter;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.bytes.ByteList;
import it.unimi.dsi.fastutil.chars.CharList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.shorts.ShortList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class QueryMoshi extends MoshiPolymorphic<IType> {

	private final Set<Class<IType>> abstractClasses;
	private final Set<Class<IType>> concreteClasses;
	private final Map<Class<?>, JsonAdapter<?>> extraAdapters;

	@SuppressWarnings({"unchecked", "RedundantCast", "rawtypes"})
	public QueryMoshi() {
		super(true, GetterStyle.RECORDS_GETTERS);
		HashSet<Class<IType>> abstractClasses = new HashSet<>();
		HashSet<Class<IType>> concreteClasses = new HashSet<>();

		// Add all super types with their implementations
		for (var superTypeClass : CurrentVersion.getSuperTypeClasses()) {
			for (Class<? extends IBaseType> superTypeSubtypesClass : CurrentVersion.getSuperTypeSubtypesClasses(
					superTypeClass)) {
				concreteClasses.add((Class<IType>) (Class) superTypeSubtypesClass);
			}
			abstractClasses.add((Class<IType>) (Class) superTypeClass);
		}

		// Add IBaseType with all basic types
		abstractClasses.add((Class<IType>) (Class) IBaseType.class);
		for (BaseType BaseType : BaseType.values()) {
			concreteClasses.add((Class<IType>) (Class) CurrentVersion.getClass(BaseType));
		}

		this.abstractClasses = abstractClasses;
		this.concreteClasses = concreteClasses;
		Object2ObjectMap<Class<?>, JsonAdapter<?>> extraAdapters = new Object2ObjectOpenHashMap<>();
		extraAdapters.put(BooleanList.class, new BooleanListJsonAdapter());
		extraAdapters.put(ByteList.class, new ByteListJsonAdapter());
		extraAdapters.put(Buf.class, new BufJsonAdapter());
		extraAdapters.put(ShortList.class, new ShortListJsonAdapter());
		extraAdapters.put(CharList.class, new CharListJsonAdapter());
		extraAdapters.put(IntList.class, new IntListJsonAdapter());
		extraAdapters.put(LongList.class, new LongListJsonAdapter());
		extraAdapters.put(IntOpenHashSet.class, new IntOpenHashSetJsonAdapter());
		this.extraAdapters = Object2ObjectMaps.unmodifiable(extraAdapters);
	}

	@Override
	public Map<Class<?>, JsonAdapter<?>> getExtraAdapters() {
		return extraAdapters;
	}

	@Override
	protected Set<Class<IType>> getAbstractClasses() {
		return abstractClasses;
	}

	@Override
	protected Set<Class<IType>> getConcreteClasses() {
		return concreteClasses;
	}

	@Override
	protected boolean shouldIgnoreField(String fieldName) {
		return fieldName.contains("$");
	}
}