package com.cinchapi.concourse;

import com.cinchapi.commons.config.AbstractFilePreferences;

/**
 * Get values for Concourse preferences. A Preference key named
 * <code>PREFERENCE_KEY_HERE</code> can be retrived by a function named
 * <code>getPreferenceKeyHere()</code>.
 * 
 * @author jnelson
 */
public class ConcoursePreferences {

	private static final String PREFS_FILE = "concourse.prefs";
	private AbstractFilePreferences prefs;

	/**
	 * Create a service to get values for the Concourse preferences.
	 */
	public ConcoursePreferences() {
		this.prefs = new AbstractFilePreferences(PREFS_FILE){};
	}

	public String getCommitLogFile(){
		return def("commit.log");
	}

	public Long getCommitLogFileMaxSizeMb(){
		return def(16L);
	}

	/**
	 * Return the defined string or default value.
	 * 
	 * @param value
	 * @return the preference value
	 */
	private String def(String value){
		return prefs.get(key(),value);
	}

	/**
	 * Return the defined long or the default value.
	 * 
	 * @param value
	 * @return the preference value
	 */
	private Long def(Long value){
		return prefs.getLong(key(),value);
	}

	/**
	 * Get the preference key based on the function name.
	 * 
	 * @return the key
	 */
	private String key(){
		String raw = new Exception().getStackTrace()[2].getMethodName().replaceFirst("get","");
		StringBuilder key = new StringBuilder();
		int count = 0;
		for(String s : raw.split("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])")){
			if(count>0){
				key.append("_");
			}
			count++;
			key.append(s.toUpperCase());
		}
		return key.toString();
	}
}
