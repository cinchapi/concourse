

public class ClientUnitTest {

	private static boolean LOGIN_SUCCESSFUL = false;
	private static final Concourse concourse = Concourse.connect();
	
	public static void main(String[] args) {
		
		
		ClientInterfaceImpl client = new ClientInterfaceImpl(concourse); 
	}
}
