package com.cinchapi.raghavbabu.userinventory;

import java.util.Scanner;

import com.cinchapi.concourse.Concourse;

/**
 * class Client
 * Class which gets the user input and perform operations on environment accordingly and
 * gives an output.
 * @author raghavbabu
 * Date :05/26/2016
 */
public class Client {
	
	private static boolean LOGIN_SUCCESSFUL = false;
	private static final Concourse concourse = Concourse.connect();
	
	/**
	 * Main method.
	 * @param args
	 */
	public static void main(String[] args) {

		ClientInterfaceImpl client = new ClientInterfaceImpl(concourse);
		
		//reading user input.
		Scanner scan = new Scanner(System.in);
		String userName = null;
		
		while(true)
		{
			int selected;

			try {
				
			System.out.println("1.Register");
			System.out.println("2.Login");
			System.out.println("3.UnRegister");
			System.out.println("4.List All Records(Requires Admin privilege)");
			System.out.println("5.Clear Inventory(Requires Admin privilege)");
			System.out.println("6.Exit");

		    selected = scan.nextInt();

			
			switch (selected) {

			case 1 : 
				System.out.println("Enter User details");
				System.out.print("Username :");
			    userName = scan.next();

				long recordId = client.duplicateUserName(userName);

				//check for duplicate userName.
				if(recordId == -1){
					System.out.println("*** UserName already exists, choose a different name ***");
					continue;
				}


				System.out.print("Password :");
				String password = scan.next();

				//check for valid password.
				while(!ClientUtil.validPassword(password) ){
					System.out.println("*** Password should have atleast 6 characters ***");
					System.out.print("Password :");
				    password = scan.next();
				}

				System.out.print("EmailId :");
				String emailId = scan.next();
				
				
				//check for valid password.
				while(!ClientUtil.validEmail(emailId) ){
					System.out.println("*** EmailId in wrong format ***");
					System.out.print("EmailId :");
				    emailId = scan.next();
				}
				

				System.out.print("Country :");
				String address = scan.next();

				System.out.print("MobileNumber :");
				String mobile = scan.next();
				System.out.println();

				User user = new User(recordId, userName, password, emailId, address, mobile);

				//check to find if register successful.
				if(client.register(user)){
					System.out.println("*** User "+userName+" successfully registered!! ***");
					LOGIN_SUCCESSFUL = true;
				}
				
				System.out.println("----------");

				break;
				
			case 2 : 
				
				System.out.print("Username :");
				userName = scan.next();

				System.out.print("Password :");
				password = scan.next();

				if(client.login(userName,password)){
					System.out.println("*** User successfully logged in!! ***");
					LOGIN_SUCCESSFUL = true;
				}else{
					LOGIN_SUCCESSFUL = false;
				}
				System.out.println("----------");
				break;
				
			case 3 : 
				
				//allow user to unregister their account only aftr login.
				if(!LOGIN_SUCCESSFUL){
					System.out.println("Please login to unregister your account");
				}else{
					client.unregister(userName);
					System.out.println("*** User "+userName+" successfully unregistered!! ***");
				}
				
			case 4 :
				client.listAllRecords();
				break;
			
			case 5 : 
				client.clearInventory();
				break;
				
			case 6 :
				System.out.println("------ Closing the Client application -------------");
				System.exit(1);

			default:
				break;
			}
		}
		catch(Exception e){
			System.out.println("Exception while reading input ");
			selected = scan.nextInt();
		}

	}

	}

}
