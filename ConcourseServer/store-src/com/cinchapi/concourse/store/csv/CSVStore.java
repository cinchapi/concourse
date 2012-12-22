package com.cinchapi.concourse.store.csv;

import java.util.Iterator;
import java.util.List;

import com.cinchapi.concourse.id.Id;
import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.model.api.Modification;
import com.cinchapi.concourse.store.ConcourseStore;
import com.cinchapi.concourse.store.ConcourseStoreTransaction;

public class CSVStore extends ConcourseStore{
	
	private static final String CSV_DELIM = ",";

	@Override
	public List<Id> query(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CSVStoreTransaction startTransaction() {
		return new CSVStoreTransaction();
	}

	@Override
	public boolean writeTransaction(ConcourseStoreTransaction transaction) {
		StringBuilder sb = new StringBuilder();
		Iterator<Modification<?>> it = transaction.iterator();
		List<String> components;
		while(it.hasNext()){
			components = it.next().asList();
			String delim = "";
			for(String component : components){
				sb.append(delim).append(component);
				delim = CSV_DELIM;
			}
			sb.append(System.getProperty("line.separator"));
		}
		System.out.println(sb.toString());
		//TODO try to append the string to a file
		return true;
	}

	@Override
	protected Entity lookup(Id id) {
		// TODO Auto-generated method stub
		return null;
	}

}
