package com.cinchapi.raghavbabu.userinventory;

/**
 * Interface ClientInterface with methods for each client to implement.
 * @author raghavbabu
 * Date :05/26/2016
 */
public interface ClientInterface {

	public boolean register(User user);
	public boolean login(String userName,String password);
	public void listAllRecords();
	public void clearInventory();
	public boolean unregister(String userName);
}
