package com.cinchapi.search;

import java.util.Map;

import com.cinchapi.common.Hash;
import com.google.common.collect.Maps;

/**
 * A String indexing service.
 * 
 * @author jnelson
 * 
 */
public class Indexer {

	/**
	 * Return a mapping from substring to index hash for {@code term}
	 * 
	 * @param term
	 * @return the indexes
	 */
	public static Map<String, String> index(String term) {
		int length = term.length();
		Map<String, String> indexes = Maps
				.newHashMapWithExpectedSize((int) Math.pow(length, 2));
		for (int i = 0; i < length; i++) {
			for (int j = i; j < length + 1; j++) {
				String substring = term.substring(i, j).trim();
				if (substring.isEmpty()) {
					continue;
				}
				String hash = Hash.md5(substring);
				indexes.put(substring, hash);
			}
		}
		return indexes;
	}

}
