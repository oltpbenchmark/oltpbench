package com.oltpbenchmark.benchmarks.twitter;

public abstract class TwitterConstants {

    public static final String TABLENAME_USER           = "user_profiles";
    public static final String TABLENAME_TWEETS         = "tweets";
    public static final String TABLENAME_FOLLOWS        = "follows";
    public static final String TABLENAME_FOLLOWERS      = "followers";
    public static final String TABLENAME_ADDED_TWEETS   = "added_tweets";
    
	/**
	 * Number of user baseline
	 */
    public static final int NUM_USERS = 500; 
    
    /**
     * Number of tweets baseline
     */
    public static final int NUM_TWEETS = 20000; 
    
    /**
     * Max follow per user baseline
     */
    public static final int MAX_FOLLOW_PER_USER = 50;

    /**
     * Message length (inclusive)
     */
    public static final int MAX_TWEET_LENGTH = 140;
    
    /**
     * Name length (inclusive)
     */
    public static final int MIN_NAME_LENGTH = 3;
    public static final int MAX_NAME_LENGTH = 20; 
	
}
