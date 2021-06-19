package it.cavallium.dbengine.client.query;

import com.squareup.moshi.JsonAdapter;
import it.cavallium.dbengine.client.IntOpenHashSetJsonAdapter;
import it.cavallium.dbengine.client.query.current.CurrentVersion;
import it.cavallium.dbengine.client.query.current.data.IBasicType;
import it.cavallium.dbengine.client.query.current.data.IType;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.bytes.ByteList;
import it.unimi.dsi.fastutil.chars.CharList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.shorts.ShortList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.warp.commonutils.moshi.BooleanListJsonAdapter;
import org.warp.commonutils.moshi.ByteListJsonAdapter;
import org.warp.commonutils.moshi.CharListJsonAdapter;
import org.warp.commonutils.moshi.IntListJsonAdapter;
import org.warp.commonutils.moshi.LongListJsonAdapter;
import org.warp.commonutils.moshi.MoshiPolymorphic;
import org.warp.commonutils.moshi.ShortListJsonAdapter;

public class QueryMoshi extends MoshiPolymorphic<IType> {

	private final Set<Class<IType>> abstractClasses;
	private final Set<Class<IType>> concreteClasses;
	private final Map<Class<?>, JsonAdapter<?>> extraAdapters;

	@SuppressWarnings({"unchecked", "RedundantCast", "rawtypes"})
	public QueryMoshi() {
		super(true, true);
		HashSet<Class<IType>> abstractClasses = new HashSet<>();
		HashSet<Class<IType>> concreteClasses = new HashSet<>();

		// Add all super types with their implementations
		for (var superTypeClass : CurrentVersion.getSuperTypeClasses()) {
			for (Class<? extends IBasicType> superTypeSubtypesClass : CurrentVersion.getSuperTypeSubtypesClasses(
					superTypeClass)) {
				concreteClasses.add((Class<IType>) (Class) superTypeSubtypesClass);
			}
			abstractClasses.add((Class<IType>) (Class) superTypeClass);
		}

		// Add IBasicType with all basic types
		abstractClasses.add((Class<IType>) (Class) IBasicType.class);
		for (BasicType basicType : BasicType.values()) {
			concreteClasses.add((Class<IType>) (Class) CurrentVersion.VERSION.getClass(basicType));
		}

		this.abstractClasses = abstractClasses;
		this.concreteClasses = concreteClasses;
		Map<Class<?>, JsonAdapter<?>> extraAdapters = new HashMap<>();
		extraAdapters.put(BooleanList.class, new BooleanListJsonAdapter());
		extraAdapters.put(ByteList.class, new ByteListJsonAdapter());
		extraAdapters.put(ShortList.class, new ShortListJsonAdapter());
		extraAdapters.put(CharList.class, new CharListJsonAdapter());
		extraAdapters.put(IntList.class, new IntListJsonAdapter());
		extraAdapters.put(LongList.class, new LongListJsonAdapter());
		extraAdapters.put(IntOpenHashSet.class, new IntOpenHashSetJsonAdapter());
		this.extraAdapters = Collections.unmodifiableMap(extraAdapters);
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