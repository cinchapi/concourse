package com.cinchapi.common.prefs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.prefs.AbstractPreferences;
import java.util.prefs.BackingStoreException;

/**
 * Abstraction for file based key/value preferences.
 * 
 * @author jnelson
 * 
 */
public abstract class AbstractFilePreferences extends AbstractPreferences {

	private final Map<String, String> prefs;
	private final File file;

	/**
	 * Construct using the specified filename.
	 * 
	 * @param filename
	 */
	public AbstractFilePreferences(String filename) {
		super(null, "");
		prefs = new HashMap<String, String>();
		file = new File(filename);
	}

	@Override
	protected void putSpi(String key, String value) {
		prefs.put(key, value);
		try {
			flush();
		} catch (BackingStoreException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected String getSpi(String key) {
		try {
			sync();
		} catch (BackingStoreException e) {
			e.printStackTrace();
		}
		return prefs.get(key);
	}

	@Override
	protected void removeSpi(String key) {
		prefs.remove(key);
		try {
			sync();
		} catch (BackingStoreException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void removeNodeSpi() throws BackingStoreException {
		throw new UnsupportedOperationException("Cannot remove node.");

	}

	@Override
	protected String[] keysSpi() throws BackingStoreException {
		return prefs.keySet().toArray(new String[prefs.keySet().size()]);
	}

	@Override
	protected String[] childrenNamesSpi() throws BackingStoreException {
		throw new UnsupportedOperationException(
				"Does not contain children nodes.");
	}

	@Override
	protected AbstractPreferences childSpi(String name) {
		throw new UnsupportedOperationException(
				"Does not contain children nodes.");
	}

	@Override
	protected void syncSpi() throws BackingStoreException {
		synchronized (file) {
			Properties props = new Properties();
			try {
				if (!file.exists()) {
					file.createNewFile();
				}
				props.load(new FileInputStream(file));
				Enumeration<?> e = props.propertyNames();
				while (e.hasMoreElements()) {
					String key = (String) e.nextElement();
					prefs.put(key, props.getProperty(key));
				}
			} catch (FileNotFoundException e) {
				throw new BackingStoreException(e);
			} catch (IOException e) {
				throw new BackingStoreException(e);
			}
		}
	}

	@Override
	protected void flushSpi() throws BackingStoreException {
		synchronized (file) {
			try {
				if (!file.exists()) {
					file.createNewFile();
				}
				Iterator<String> it = prefs.keySet().iterator();
				StringBuilder sb = new StringBuilder();
				while (it.hasNext()) {
					String key = it.next();
					String value = prefs.get(key);
					sb.append(key).append(" = ").append(value)
							.append(System.getProperty("line.separator"));
				}
				PrintWriter writer = new PrintWriter(new BufferedWriter(
						new FileWriter(file.getAbsolutePath())));
				writer.print(sb);
				writer.close();
			} catch (IOException e) {
				throw new BackingStoreException(e);
			}
		}
	}

}
