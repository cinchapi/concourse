package com.cinchapi.common;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Provides common hash implementations.
 * 
 * @author jnelson
 * 
 */
public class Hash {

	/**
	 * Return the MD5 hash string for {@code message}.
	 * 
	 * @param message
	 * @return the hash string
	 */
	public static String md5(String message) {
		return toString(md5(message.getBytes()));
	}

	/**
	 * Return the MD5 hash for {@code message}.
	 * 
	 * @param message
	 * @return the hash
	 */
	public static byte[] md5(byte[] message) {
		return hash("MD5", message);
	}

	/**
	 * Return the SHA-1 hash string for {@code message}
	 * 
	 * @param message
	 * @return the hash string
	 */
	public static String sha1(String message) {
		return toString(sha1(message.getBytes()));
	}

	/**
	 * Return the SHA-256 hash for the {@code messages}
	 * 
	 * @param messages
	 * @return the hash
	 */
	public static byte[] sha256(byte[]... messages) {
		return hash("SHA-256", messages);
	}

	/**
	 * Return the SHA-256 hash string for {@code message}
	 * 
	 * @param message
	 * @return the hash string
	 */
	public static String sha256(String message) {
		return toString(sha256(message.getBytes()));
	}

	/**
	 * Return the SHA-1 hash for {@code message}
	 * 
	 * @param message
	 * @return the hash
	 */
	public static byte[] sha1(byte[] message) {
		return hash("SHA-1", message);
	}

	/**
	 * Return the SHA-512 hash for {@code message}.
	 * 
	 * @param message
	 * @return the hash
	 */
	public static byte[] sha512(byte[] message) {
		return hash("SHA-512", message);
	}

	/**
	 * Return the SHA-512 hash string for {@code message}.
	 * 
	 * @param message
	 * @return the hash string
	 */
	public static String sha512(String message) {
		return toString(sha512(message.getBytes()));
	}

	/**
	 * Generate a hash for the <code>messages<code> using {@code algorithm}
	 * .
	 * 
	 * @param algorithm
	 * @param messages
	 * @return the hash
	 */
	private static byte[] hash(String algorithm, byte[]... messages) {
		MessageDigest md;
		try {
			md = MessageDigest.getInstance(algorithm);
			for (byte[] message : messages) {
				md.update(message);
			}
			return md.digest();
		}
		catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;

	}

	/**
	 * Convert {@code hash} from a byte array to a string.
	 * 
	 * @param hash
	 * @return the string representation of {@code hash}
	 */
	public static String toString(byte[] hash) {
		StringBuilder hex = new StringBuilder();
		for (int i = 0; i < hash.length; i++) {
			hex.append(Integer.toHexString(0xFF & hash[i]));
		}
		return hex.toString();
	}

	public static void main(String[] args) {
		System.out.println(Hash.toString(Hash.sha256("jeff".getBytes(), "nelson".getBytes())));
		System.out.println(Hash.toString(Hash.sha256("jeff".getBytes(), "nelson".getBytes())));
		System.out.println(Hash.toString(Hash.sha256("nelson".getBytes())));
	}
}
