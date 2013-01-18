package com.cinchapi.concourse.store;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import com.cinchapi.commons.util.Files;
import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.Id;
import com.cinchapi.concourse.model.Revision;

public class ModificationLog extends TemporaryStore {

	private final String filename;

	/**
	 * Create a new {@link ModificationLog}.
	 * 
	 * @param filename
	 */
	public ModificationLog(String filename) {
		this.filename = filename;
	}

	@Override
	public List<Id> query(String query){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Entity read(Id id){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Revision<?> read(String lookup){
		// TODO Auto-generated method stub
		return null;
	}

	/** Write lines of JSON to the commit log. */
	@Override
	public void write(Revision<?> mod){
		if(mod!=null){
			try{
				new PrintWriter(new BufferedWriter(
						new FileWriter(filename,true))).println(mod);
			}
			catch (IOException e){
				e.printStackTrace();
			}

		}
	}

	@Override
	public void flush(PersistentStore store){
		// TODO Auto-generated method stub
	}

	/**
	 * Return the size of the commit log in MB.
	 * 
	 * @return the size
	 */
	public Long sizeMB(){
		return Files.mb(filename);
	}

}
