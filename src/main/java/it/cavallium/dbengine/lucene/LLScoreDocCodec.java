package it.cavallium.dbengine.lucene;

import io.netty5.buffer.Buffer;
import java.util.function.Function;

public class LLScoreDocCodec implements HugePqCodec<LLScoreDoc> {

	@Override
	public Buffer serialize(Function<Integer, Buffer> allocator, LLScoreDoc data) {
		var buf = allocator.apply(Float.BYTES + Integer.BYTES + Integer.BYTES);
		buf.writerOffset(Float.BYTES + Integer.BYTES + Integer.BYTES);
		setScore(buf, data.score());
		setDoc(buf, data.doc());
		setShardIndex(buf, data.shardIndex());
		return buf;
	}

	@Override
	public LLScoreDoc deserialize(Buffer buf) {
		return new LLScoreDoc(getDoc(buf), getScore(buf), getShardIndex(buf));
	}

	private static float getScore(Buffer hit) {
		return HugePqCodec.getLexFloat(hit, 0, false);
	}

	private static int getDoc(Buffer hit) {
		return HugePqCodec.getLexInt(hit, Float.BYTES, true);
	}

	private static int getShardIndex(Buffer hit) {
		return HugePqCodec.getLexInt(hit, Float.BYTES + Integer.BYTES, false);
	}

	private static void setScore(Buffer hit, float score) {
		HugePqCodec.setLexFloat(hit, 0, false, score);
	}

	private static void setDoc(Buffer hit, int doc) {
		HugePqCodec.setLexInt(hit, Float.BYTES, true, doc);
	}

	private static void setShardIndex(Buffer hit, int shardIndex) {
		HugePqCodec.setLexInt(hit, Float.BYTES + Integer.BYTES, false, shardIndex);
	}

	@Override
	public LLScoreDoc clone(LLScoreDoc obj) {
		return new LLScoreDoc(obj.doc(), obj.score(), obj.shardIndex());
	}
}
