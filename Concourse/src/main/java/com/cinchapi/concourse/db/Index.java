/*
 * This project is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This project is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this project. If not, see <http://www.gnu.org/licenses/>.
 */
package com.cinchapi.concourse.db;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cinchapi.common.util.RandomString;
import com.cinchapi.common.util.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * 
 * 
 * @author jnelson
 */
public class Index extends BucketMap<ByteSizedString, PrimaryKey>{

	/**
	 * Construct a new instance.
	 * @param locator
	 */
	protected Index(ByteSizedString index) {
		super(index);
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.db.Store#getBucketFromByteSequence(java.nio.ByteBuffer)
	 */
	@Override
	protected Bucket<ByteSizedString, PrimaryKey> getBucketFromByteSequence(
			ByteBuffer bytes) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.db.Store#getMockBucket()
	 */
	@Override
	protected Bucket<ByteSizedString, PrimaryKey> getMockBucket() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.db.Store#getNewBucket(com.cinchapi.concourse.io.ByteSized)
	 */
	@Override
	protected Bucket<ByteSizedString, PrimaryKey> getNewBucket(
			ByteSizedString key) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.db.Store#getNewBuckets(int)
	 */
	@Override
	protected Map<ByteSizedString, Bucket<ByteSizedString, PrimaryKey>> getNewBuckets(
			int expectedSize) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static void main(String... args){
		String string = new RandomString().nextString();
		System.out.println("the length is "+string.length());
		System.out.println("the string is "+string);
		
		Set<String> m1 = Sets.newLinkedHashSet();
		Set<String> m2 = Sets.newLinkedHashSet();
		
		//m1: all substrings
		m1 = index(string);
		System.out.println(m1);
		System.out.println(m1.size());
		
		//m2: word substrings
		String[] toks = string.split(" ");
		for(String tok : toks){
			m2.addAll(index(tok));
		}
		System.out.println(m2);
		System.out.println(m2.size());
	}
	
	static Set<String> index(String string){
		Set<String> set = Sets.newLinkedHashSet();
		for(int i = 0; i < string.length(); i++){
			for(int j = i+1; j < string.length()+1; j++){
				String index = string.substring(i, j);
				if(index != " "){
					set.add(string.substring(i, j));
				}
			}
		}
		return set;
	}

}
