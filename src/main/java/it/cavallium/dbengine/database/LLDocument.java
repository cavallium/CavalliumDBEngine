package it.cavallium.dbengine.database;

import java.util.Arrays;
import org.jetbrains.annotations.Nullable;

public class LLDocument {

	private final LLItem[] items;

	public LLDocument(LLItem[] items) {
		this.items = items;
	}

	public LLItem[] getItems() {
		return items;
	}

	@Override
	public String toString() {
		return Arrays.toString(items);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LLDocument that = (LLDocument) o;
		return Arrays.equals(items, that.items);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(items);
	}

	@Nullable
	public LLItem getField(String uid) {
		for (LLItem item : items) {
			if (item.getName().equals(uid)) {
				return item;
			}
		}
		return null;
	}
}
