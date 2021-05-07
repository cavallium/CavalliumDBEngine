package it.cavallium.dbengine.client.query;

import it.cavallium.dbengine.client.query.current.CurrentVersion;
import it.cavallium.dbengine.client.query.current.data.IBasicType;
import it.cavallium.dbengine.client.query.current.data.IType;
import java.util.HashSet;
import java.util.Set;
import org.warp.commonutils.moshi.MoshiPolymorphic;

public class QueryMoshi extends MoshiPolymorphic<IType> {

	private final Set<Class<IType>> abstractClasses;
	private final Set<Class<IType>> concreteClasses;

	@SuppressWarnings({"unchecked", "RedundantCast", "rawtypes"})
	public QueryMoshi() {
		HashSet<Class<IType>> abstractClasses = new HashSet<>();
		HashSet<Class<IType>> concreteClasses = new HashSet<>();
		for (var superTypeClass : CurrentVersion.getSuperTypeClasses()) {
			for (Class<? extends IBasicType> superTypeSubtypesClass : CurrentVersion.getSuperTypeSubtypesClasses(
					superTypeClass)) {
				concreteClasses.add((Class<IType>) (Class) superTypeSubtypesClass);
			}
			abstractClasses.add((Class<IType>) (Class) superTypeClass);
		}
		this.abstractClasses = abstractClasses;
		this.concreteClasses = concreteClasses;
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
		return false;
	}
}