package com.cinchapi.concourse.id;

/**
 * A {@link Comparable} 64-bit numeric {@link Id}.
 * @author jnelson
 *
 */
public class LongId implements Id, Comparable<LongId>{
	
	Long id;
	
	/**
	 * Create a new {@link LongId}.
	 * @param id
	 */
	public LongId(Long id){
		this.id = id;
	}

	@Override
	public int compareTo(LongId arg0) {
		return this.id.compareTo(arg0.id);
	}

}
