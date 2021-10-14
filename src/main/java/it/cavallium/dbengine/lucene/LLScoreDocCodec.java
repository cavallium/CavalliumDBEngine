package it.cavallium.dbengine.lucene;

import io.net5.buffer.ByteBuf;
import java.util.function.Function;

public class LLScoreDocCodec implements LMDBSortedCodec<LLScoreDoc> {

	@Override
	public ByteBuf serialize(Function<Integer, ByteBuf> allocator, LLScoreDoc data) {
		var buf = allocator.apply(Float.BYTES + Integer.BYTES + Integer.BYTES);
		setScore(buf, data.score());
		setDoc(buf, data.doc());
		setShardIndex(buf, data.shardIndex());
		buf.writerIndex(Float.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES);
		return buf.asReadOnly();
	}

	@Override
	public LLScoreDoc deserialize(ByteBuf buf) {
		return new LLScoreDoc(getDoc(buf), getScore(buf), getShardIndex(buf));
	}

	@Override
	public int compare(LLScoreDoc hitA, LLScoreDoc hitB) {
		if (hitA.score() == hitB.score()) {
			if (hitA.doc() == hitB.doc()) {
				return Integer.compare(hitA.shardIndex(), hitB.shardIndex());
			} else {
				return Integer.compare(hitB.doc(), hitA.doc());
			}
		} else {
			return Float.compare(hitA.score(), hitB.score());
		}
	}

	@Override
	public int compareDirect(ByteBuf hitA, ByteBuf hitB) {
		var scoreA = getScore(hitA);
		var scoreB = getScore(hitB);
		if (scoreA == scoreB) {
			var docA = getDoc(hitA);
			var docB = getDoc(hitB);
			if (docA == docB) {
				return Integer.compare(getShardIndex(hitA), getShardIndex(hitB));
			} else {
				return Integer.compare(docB, docA);
			}
		} else {
			return Float.compare(scoreA, scoreB);
		}
	}

	private static float getScore(ByteBuf hit) {
		return hit.getFloat(0);
	}

	private static int getDoc(ByteBuf hit) {
		return hit.getInt(Float.BYTES);
	}

	private static int getShardIndex(ByteBuf hit) {
		return hit.getInt(Float.BYTES + Integer.BYTES);
	}

	private static void setScore(ByteBuf hit, float score) {
		hit.setFloat(0, score);
	}

	private static void setDoc(ByteBuf hit, int doc) {
		hit.setInt(Float.BYTES, doc);
	}

	private static void setShardIndex(ByteBuf hit, int shardIndex) {
		hit.setInt(Float.BYTES + Integer.BYTES, shardIndex);
	}
}
