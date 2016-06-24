

/**
 * class User
 * Holds user details for each user.
 * @author raghavbabu
 * Date :05/26/2016
 */
public class User {

	private long userId;
	private String username;
	private String password;
	private String emailId;
	private String country;
	private String mobileNumber;

	public User(long userId, String username, String password, String emailId,
			String country, String mobileNumber) {
		this.userId = userId;
		this.username = username;
		this.password = password;
		this.emailId = emailId;
		this.country = country;
		this.mobileNumber = mobileNumber;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}
	
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getEmailId() {
		return emailId;
	}
	public void setEmailId(String emailId) {
		this.emailId = emailId;
	}

	public String getMobileNumber() {
		return mobileNumber;
	}
	public void setMobileNumber(String mobileNumber) {
		this.mobileNumber = mobileNumber;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

}
