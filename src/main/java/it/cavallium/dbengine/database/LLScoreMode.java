package it.cavallium.dbengine.database;

public enum	LLScoreMode {
	/**
	 * Produced scorers will allow visiting all matches and get their score.
	 */
	COMPLETE,
	/**
	 * Produced scorers will allow visiting all matches but scores won't be
	 * available.
	 * Much faster in multi-lucene indices than complete, because it will not need global scores calculation.
	 */
	COMPLETE_NO_SCORES,
	/**
	 * Produced scorers will optionally allow skipping over non-competitive
	 * hits using the Scorer#setMinCompetitiveScore(float) API.
	 * This can reduce time if using setMinCompetitiveScore.
	 */
	TOP_SCORES,
	/**
	 * Produced scorers will allow visiting some matches but scores won't be
	 * available.
	 * Much faster in multi-lucene indices than complete, because it will not need global scores calculation.
	 */
	NO_SCORES
}
