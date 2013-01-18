package com.cinchapi.concourse.model.id;

import java.security.SecureRandom;
import java.util.List;

import com.cinchapi.commons.util.Counter;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;

public class SimpleIDService implements IDService{
	
	private SecureRandom random;
	private List<UnsignedLong> provided = Lists.newArrayList();
	private Counter counter = new Counter();
	
	public SimpleIDService(){
		random = new SecureRandom();
		random.setSeed(random.generateSeed(8));
	}

	@Override
	public UnsignedLong requestRandom(){
		UnsignedLong id = generateRandomLong();
		while(provided.contains(id)){
			id = generateRandomLong();
		}
		provided.add(id);
		return id;
	}

	@Override
	public UnsignedLong requestSequential(){
		UnsignedLong id = UnsignedLong.valueOf(counter.next());
		while(provided.contains(id)){
			id = UnsignedLong.valueOf(counter.next());
		}
		provided.add(id);
		return id;
	}
	
	private Long byteArrayToLong(byte[] bytes){
		long value = 0 ;
		for (int i = 0; i < bytes.length; i++)
		{
		   value += ((long) bytes[i] & 0xffL) << (8 * i);
		}
		return value;
	}
	
	private UnsignedLong generateRandomLong(){
		byte[] bytes = new byte[8];
		random.nextBytes(bytes);
		return UnsignedLong.fromLongBits(byteArrayToLong(bytes));
	}

}
