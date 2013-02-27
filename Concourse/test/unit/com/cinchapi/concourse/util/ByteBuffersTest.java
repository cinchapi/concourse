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
package com.cinchapi.concourse.util;

import java.util.Random;

import org.junit.Test;

import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.util.RandomString;

import junit.framework.TestCase;

/**
 * Unit tests for {@link ByteBuffers}.
 * 
 * @author jnelson
 */
public class ByteBuffersTest extends TestCase{
	
	private static Random rand = new Random();
	private static RandomString strand = new RandomString();
	
	private Boolean getBooleanValue(){
		return rand.nextInt() % 2 == 0 ? true : false;
	}
	
	private Double getDoubleValue(){
		return rand.nextDouble();
	}
	
	private Float getFloatValue(){
		return rand.nextFloat();
	}
	
	private Integer getIntValue(){
		return rand.nextInt();
	}
	
	private Long getLongValue(){
		return rand.nextLong();
	}
	
	private String getStringValue(){
		return strand.nextString();
	}
	
	@Test
	public void testBoolean(){
		Boolean value = getBooleanValue();
		assertEquals(value, ByteBuffers.getBoolean(ByteBuffers.toByteBuffer(value)));
	}
	
	@Test
	public void testDouble(){
		Double value = getDoubleValue();
		assertEquals(value, ByteBuffers.getDouble(ByteBuffers.toByteBuffer(value)));
	}
	
	@Test
	public void testFloat(){
		Float value = getFloatValue();
		assertEquals(value, ByteBuffers.getFloat(ByteBuffers.toByteBuffer(value)));
	}
	
	@Test
	public void testInt(){
		Integer value = getIntValue();
		assertEquals(value, ByteBuffers.getInt(ByteBuffers.toByteBuffer(value)));
	}
	
	@Test
	public void testLong(){
		Long value = getLongValue();
		assertEquals(value, ByteBuffers.getLong(ByteBuffers.toByteBuffer(value)));
	}
	
	@Test
	public void testString(){
		String value = getStringValue();
		assertEquals(value, ByteBuffers.getString(ByteBuffers.toByteBuffer(value)));
	}

}
