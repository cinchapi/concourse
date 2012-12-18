package com.cinchapi.concourse.idgen.snowflake;


import com.cinchapi.concourse.idgen.IDGenerator;

public class SnowflakeService implements IDGenerator<Long>{

	@Override
	public Long requestId() {
		return 1L;
	}

}
