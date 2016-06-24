

import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class ClientUtil
 * class which holds utility methods required for client input validation.
 * @author raghavbabu
 * Date :05/26/2016
 */

public final class ClientUtil {


	private static final String EMAIL_PATTERN = 
			"^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
					+ "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";


	/**
	 * Password length should atleast have 6 characters. else return false.
	 */
	public static boolean validPassword(String password) {
		return (password.length() < 6) ? false : true;
	}

	/**
	 * Method which returns max id from the inventory for the newly registering user.
	 * @param recordIds
	 * @return long.
	 */
	public static long findMax(Set<Long> recordIds) {

		Iterator<Long> it = recordIds.iterator();

		long max = 0;

		while(it.hasNext()){

			long id = it.next();

			if(id > max){
				max = id; 
			}
		}


		return max;
	}

	/**
	 * Method to validate an email.
	 * @param emailId
	 * @return true if valid email, else false.
	 */
	public static boolean validEmail(String emailId) {

		Pattern pattern = Pattern.compile(EMAIL_PATTERN);

		Matcher matcher = pattern.matcher(emailId);
		return matcher.matches();

	}

}
