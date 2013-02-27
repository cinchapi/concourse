package com.cinchapi.concourse.model;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import com.cinchapi.concourse.db.CellValue;
import com.cinchapi.concourse.db.RowKey;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.util.AtomicClock;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;
import com.javadocmd.simplelatlng.LatLng;

public class Test {
	
	public static void main(String[] args) throws InterruptedException{
		
		Concourse concourse = new HeapBackedConcourse(5);
//		List<Object> values = Lists.newArrayList();
//		values.add("wmfdt7jed 8salcxs9xb085mbk4me");
//		values.add(0.4773719818121055);
//		values.add(0.7639882397105777);
//		values.add(0.14093864728135552);
//		values.add("tm8p1v9k9  ef5bwos4qhw3v 50kzwanv pa4u 32q huyo   j57re59tr2vg2fv6mv7pa145k");
//		values.add("8c0qxthm5ltmywhh3falvshve  0qzp5yh0iso91gpu 1i342otlnps 8");
//		values.add(2588129207812480705L);
//		values.add(0.044490933);
//		values.add("we7u9a869krw92jutt97ty307u0xnmfux01kwvlfs4yriiefyorcc15zxmm940da5ivs");
//		values.add(false);
//		values.add(1739852798);
//		values.add("you");
//		
//		Iterator<Object> it = values.iterator();
//		while(it.hasNext()){
//			Object value = it.next();
//			if(concourse.add(UnsignedLong.valueOf(1), "name", value)){
//				System.out.println(value+" worked");
//			}
//		}
//		UnsignedLong row = UnsignedLong.valueOf(1);
//		String column = "name";
//		String value = "Jeff";
//		concourse.add(row, column, value);
//		System.out.println(concourse);
		
		System.out.println(CellValue.notForStorage(new LatLng(2, 3)));
	
	}

}
