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
package com.cinchapi.concourse.server;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.ConcourseConstants;
import com.cinchapi.concourse.auth.AttemptedAuth;
import com.cinchapi.concourse.auth.AttemptedAuthResponse;
import com.cinchapi.concourse.auth.CredsFile;
import com.cinchapi.concourse.client.ClientRequest;
import com.cinchapi.concourse.exception.ConcourseException;
import com.cinchapi.concourse.internal.Engine;

/**
 * Base implementation of the {@ConcourseServer} interface.
 * 
 * @author jnelson
 */
public abstract class AbstractConcourseServer implements Server {

	protected final Logger log = LoggerFactory.getLogger(this.getClass());
	private static final String CREDS_FILE_NAME = ".creds";

	// Defaults
	private static final String HOST_DEFAULT = "localhost";
	private static final int PORT_DEFAULT = 1717;
	private static final int MAX_CONNECTIONS_DEFAULT = 50;
	private static final String HOME_DEFAULT = System.getProperty("user.home")
			+ File.separator + "concourse";

	// Provided
	private final String host;
	private final int port;
	private final int maxConnections;

	// Derived
	private final File home;
	private ServerSocket serverSocket;
	private Engine concourse;
	private CredsFile creds;

	/**
	 * Construct a new default instance.
	 */
	protected AbstractConcourseServer() {
		this(HOST_DEFAULT, PORT_DEFAULT, MAX_CONNECTIONS_DEFAULT, HOME_DEFAULT);
	}

	/**
	 * Construct a new configured instance.
	 * 
	 * @param host
	 * @param port
	 * @param maxConnections
	 * @param home
	 */
	protected AbstractConcourseServer(String host, int port,
			int maxConnections, String home) {
		this.host = host;
		this.port = port;
		this.maxConnections = maxConnections;
		this.home = new File(home);
	}

	@Override
	public String cal(String statement) throws ConcourseException {
		return "that was cool";
	}

	@Override
	public boolean login(String username, String password) {
		return password.equals(creds.getPasswordFor(username));
	}

	@Override
	public void start() {
		log.info(
				"Starting the server hosted at {} and listening on port {} with a home in {}",
				host, port, home.getAbsolutePath());
		try {
			serverSocket = new ServerSocket(port, maxConnections,
					InetAddress.getByName(host));
		}
		catch (IOException e) {
			log.error("An error occured while starting the server", e);
			System.exit(-1);
		}

		// Load the creds to handle authentication
		try {
			File credsFile = new File(home + File.separator + CREDS_FILE_NAME);
			if(!credsFile.exists()) {
				log.warn("Is this a new server? No creds file located at {}",
						credsFile.getAbsolutePath());
				creds = CredsFile.newInstance(credsFile.getAbsolutePath());
				log.info("Installed a new default creds file at {}",
						credsFile.getAbsoluteFile());
			}
			else {
				creds = CredsFile.fromExisting(credsFile.getAbsolutePath());
			}
		}
		catch (Throwable t) {
			log.error("An error occured while starting the server", t);
			System.exit(-1);
		}

		// Load Concourse
		concourse = Engine.withHomeAt(home.getAbsolutePath());
		log.info("The server has successfully started");
		accept();
	}

	@Override
	public void stop() {
		System.exit(0);

	}

	/**
	 * Return a {@link Runnable} to handle the client connection.
	 * 
	 * @param clientSocket
	 * @return the client handler
	 */
	protected abstract Runnable getClientHandler(Socket clientSocket);

	/**
	 * Wait for and accept and client connections
	 */
	private void accept() {
		try {
			while (!serverSocket.isClosed()) {
				Socket clientSocket = serverSocket.accept();
				Runnable clientHandler = getClientHandler(clientSocket);
				Thread clientThread = new Thread(clientHandler);
				clientThread.start();
			}
		}
		catch (IOException e) {
			log.warn("", e);
		}
	}

	/**
	 * The default implementation for what should be retuened from
	 * {@link AbstractConcourseServer#getClientHandler(Socket)}
	 * 
	 * @author jnelson
	 */
	public class DefaultClientHandler implements Runnable {

		private final Socket clientSocket;
		private ObjectOutputStream out;
		private ObjectInputStream in;

		public DefaultClientHandler(Socket clientSocket) {
			this.clientSocket = clientSocket;
		}

		@Override
		public void run() {
			try {
				in = new ObjectInputStream(clientSocket.getInputStream());
				out = new ObjectOutputStream(clientSocket.getOutputStream());

				// read the auth details from the client and send a response
				AttemptedAuth creds = (AttemptedAuth) in.readObject();
				if(login(creds.getUsername(), creds.getPassword())) {
					out.writeObject(new AttemptedAuthResponse(true));
					String message = "Concourse Server "
							+ ConcourseConstants.VERSION + "."
							+ System.lineSeparator();
					out.writeObject(new InitialMessage(message));

					while (!clientSocket.isClosed()) {
						ClientRequest request = (ClientRequest) in.readObject();
						ServerResponse response;
						if(request.getContent().equalsIgnoreCase("exit")) {
							response = new DroppedClientResponse();
							out.writeObject(response);
							drop();
						}
						else {
							// TODO change the request into a CQL statement and
							// process it, etc
							response = new ServerResponse(
									"I am confirming that your request was "
											+ request);
							out.writeObject(response);
						}

					}
				}
				else {
					out.writeObject(new AttemptedAuthResponse(false));
					drop();
				}
			}
			catch (EOFException e) {} // this happens when clients hard
			// disconnect so we can ignore it
			catch (Throwable t) {
				log.warn("", t);
			}

		}

		/**
		 * Drop the client connection.
		 * 
		 * @throws Throwable
		 */
		private void drop() throws Throwable {
			clientSocket.close();
			in.close();
			out.close();
		}

	}

}
