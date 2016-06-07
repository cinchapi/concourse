package com.cinchapi.raghavbabu.userinventory;


import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;

import com.cinchapi.concourse.Concourse;

/**
 * class ClientInterfaceImpl
 * Has implemented methods for client.Performs all processes to retrieve,
 * add,remove from environment.
 * @author raghavbabu
 * Date :05/26/2016
 */
public class ClientInterfaceImpl implements ClientInterface {

	
	private Concourse concourse;
	
	public ClientInterfaceImpl(Concourse concourse){
		this.concourse = Concourse.connect();
	}
	/**
	 * Method to login a user.
	 * Check for username,password match by reading from environment. 
	 * If matches,return true else returns false.
	 */
	@Override
	public boolean login(String userName, String password) {


		Set<Long> recordIds = concourse.find("UserName", userName);

		Iterator<Long> it = recordIds.iterator();
		
		//if no record with username present. Prompt to register.
		if(recordIds.isEmpty()){
			System.out.println("*** UserName doesn't exist,Please register. ***");
			return false;
		}
		else{
			
			//get password in respective user record id.
			long id = it.next();
			String passwdinDB = concourse.get("Password", id);

			//if user present, validate their password with password from environment.
			if(password.equals(passwdinDB) )
				return true;
			else{
				System.out.println("*** Invalid username or password ***");
				return false;
			}
			
		}
						
	}
	
	/**
	 * List all records which are added to the environment.
	 * Prints all record values added.
	 */
	@Override
	public void listAllRecords() {
		
		//to get all records.
		Iterator<Long> it = concourse.inventory().iterator();
		
		System.out.println("--------User Details-----------");
		
		while(it.hasNext()){
			long id = it.next();
			String name = concourse.get("UserName",id);
			
			if(name != null){
			System.out.print(name+", "+concourse.get("EmailId",id)
					+", "+ concourse.get("Country",id)+", "+concourse.get("MobileNo",id));
			System.out.println();
			}
		}
		System.out.println("---------------------------------");
	}
	
	/**
	 * clear the inventory.
	 * clear each record values.
	 */
	@Override
	public void clearInventory() {
		
		//get all record ids.
		Iterator<Long> it = concourse.inventory().iterator();
		
		//clearing all the key value pairs in each record.
		while(it.hasNext()){
			long id = it.next();
			concourse.clear(id);
		}
	}

	/**
	 * Registering a user into environment.
	 * return true after registering the user.
	 */
	@Override
	public boolean register(User user) {

		//each time when I add a new record, I will get the maximum record value and increment it by 1 and store 
		//the user details.
		long recordId = user.getUserId() + 1;
		concourse.set("UserName", user.getUsername(), recordId);
		concourse.set("Password",user.getPassword(), recordId);
		concourse.set("EmailId",user.getEmailId(),recordId);
		concourse.set("Country",user.getCountry(),recordId);
		concourse.set("MobileNo",user.getMobileNumber(),recordId);
		
		return true;
	}
	
	/**
	 * Unregister a user from environment.
	 * return true if user unregistered, false otherwise.
	 */
	@Override
	public boolean unregister(String userName) {
		
		//get the record id which has the userName. 
		Set<Long> recordIds = concourse.find("UserName", userName);
		
		
		Iterator<Long> it = recordIds.iterator();
		
		if(recordIds.isEmpty()){
			System.out.println("*** UserName not yet registered ***");
			return false;
		}else{
			long id = it.next();
			concourse.clear(id); // removing all key's value from this record. 
			
			//I wanted to nuke() the complete record, but the functionality is not available.
			//recordIds.nuke();
		}
				
		return true;
	}

	

	/**
	 *Method to check if the same userName is already present in the environment.
	 *adds to environment and returns record Id if user not present, else return -1.
	 *return record id or -1 if record  with username not present.
	 */
	public long duplicateUserName(String userName) {

		
		Set<Long> allRecordIds = concourse.inventory();
		
		//each time when I add a new record, I will get the maximum record value.
		long recordId = ClientUtil.findMax(allRecordIds);
		
		Set<Long> recordIds = concourse.find("UserName", userName);
		
		Iterator<Long> it = recordIds.iterator();
		
		//if user name already present...return -1.
		if(!recordIds.isEmpty()){
			recordId = -1;
		}
		
		//else return new id.
		return recordId;
	}


}
