package it.cavallium.dbengine.lucene;

public interface Reversable<T extends Reversable<T>> {

	T reverse();
}
