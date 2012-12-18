package com.cinchapi.concourse.model.mock;

import com.cinchapi.concourse.id.Id;

public class MockLongId implements Id, Comparable<MockLongId>{
	
	private Long id;
	
	public MockLongId(Long id){
		this.id = id;
	}

	@Override
	public int compareTo(MockLongId arg0) {
		return id.compareTo(arg0.id);
	}

}
