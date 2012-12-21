package com.cinchapi.concourse.store;

import java.util.Iterator;
import java.util.List;

import com.cinchapi.concourse.id.Id;
import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.property.StringProperty;
import com.cinchapi.concourse.store.api.Mutation;

public class CsvFileStore extends AbstractStore<CsvFileStoreTransaction>{
	
	private static final String CSV_DELIM = ",";

	@Override
	public List<Id> query(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Entity load(Id id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CsvFileStoreTransaction startTransaction() {
		return new CsvFileStoreTransaction();
	}

	@Override
	public boolean writeTransaction(CsvFileStoreTransaction transaction) {
		StringBuilder sb = new StringBuilder();
		Iterator<Sha1LocatorMutation> it = transaction.iterator();
		List<String> mutationComponents;
		while(it.hasNext()){
			mutationComponents = it.next().asList();
			String delim = "";
			for(String component : mutationComponents){
				sb.append(delim).append(component);
				delim = CSV_DELIM;
			}
			sb.append(System.getProperty("line.separator"));
		}
		System.out.println(sb.toString());
		//TODO try to append the string to a file
		return true;
	}

	public static void main(String[] args){
		CsvFileStore store = new CsvFileStore();
		CsvFileStoreTransaction t = store.startTransaction();
		t.record(new Sha1LocatorMutation(new Id("1"), new StringProperty("name", "Jeff Nelson"), Mutation.Type.ADDITION));
		t.record(new Sha1LocatorMutation(new Id("5"), new StringProperty("name", "Jeff Nelson"), Mutation.Type.ADDITION));
		store.writeTransaction(t);
	}


}
