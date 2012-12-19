package com.cinchapi.concourse.id;

/**
 * A value that can be used to identify an {@link Entity}.
 * @author jnelson
 *
 */
public class Id implements Comparable<Id>{
	
	String id;
	
	/**
	 * Create a new {@link Id}.
	 * @param id
	 */
	public Id(String id){
		this.id = id;
	}

	@Override
	public int compareTo(Id o) {
		return this.id.compareTo(o.id);
	}
}
