package it.cavallium.dbengine.lucene;

public class LMDBThread extends Thread {

	public LMDBThread(Runnable r) {
		super(r);
	}
}
