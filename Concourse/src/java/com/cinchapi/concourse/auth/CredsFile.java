/*
 * This project is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This project is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this project. If not, see <http://www.gnu.org/licenses/>.
 */
package com.cinchapi.concourse.auth;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.ConcourseConstants;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

/**
 * A file that contains valid credentials for accessing the
 * {@link ConcourseServer}.
 * 
 * @author jnelson
 */
public class CredsFile implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final String ROOT_USER = "root";
	private static final String ROOT_PW = AuthUtils.hashPassword(ROOT_USER,
			"root", ConcourseConstants.DEFAULT_SERVER_SALT);

	private static final Logger log = LoggerFactory.getLogger(CredsFile.class);

	/**
	 * Return an existing creds file that is stored in {@code file}.
	 * 
	 * @param file
	 * @return the creds file
	 * @throws Throwable
	 */
	public static CredsFile fromExisting(String file) throws Throwable {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
		CredsFile cf = (CredsFile) in.readObject();
		in.close();
		return cf;
	}

	/**
	 * Return a new creds file.
	 * 
	 * @param file
	 * @return the creds file
	 * @throws Throwable
	 */
	public static CredsFile newInstance(String file) throws Throwable {
		Map<String, String> creds = Maps.newHashMap();
		creds.put(ROOT_USER, ROOT_PW);
		CredsFile cf = new CredsFile(creds);
		cf.flush(file);
		return cf;
	}

	private final Map<String, String> creds;

	/**
	 * Construct a new instance.
	 * 
	 * @param creds
	 */
	private CredsFile(Map<String, String> creds) {
		this.creds = creds;
	}

	/**
	 * Return the password for {@code user}.
	 * 
	 * @param user
	 * @return the password
	 */
	public String getPasswordFor(String user) {
		if(creds.containsKey(user)) {
			String password = creds.get(user);
			if(user.equals(ROOT_USER) && password.equals(ROOT_PW)) {
				log.warn("Your server is insecure. You should change the root password ASAP.");
			}
			return password;
		}
		log.warn("Attemped to get password for a non-existing user {}", user);
		throw new SecurityException("Unknown user");

	}

	/**
	 * Flush out any changes to the creds file.
	 * 
	 * @param file
	 * @throws Throwable
	 */
	public void flush(String file) throws Throwable {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(
				file));
		out.writeObject(this);
		out.flush();
		out.close();
	}

}
