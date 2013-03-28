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
package com.cinchapi.common;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Additional utility methods for Strings.
 * 
 * @author jnelson
 */
public class Strings {

	/**
	 * Return {@code true} if {@code string} contains any whitespace characters.
	 * 
	 * @param string
	 * @return {@code true} if the string contains whitespace
	 */
	public static boolean containsWhitespace(String string) {
		Pattern pattern = Pattern.compile("\\s");
		Matcher matcher = pattern.matcher(string);
		return matcher.find();
	}

	/**
	 * Return {@code true} if {@code string} contains any of the specified
	 * {@code chars}.
	 * 
	 * @param string
	 * @param chars
	 * @return {@code true} if any {@code chars} are contained.
	 */
	public static boolean contains(String string, CharSequence... chars) {
		for (CharSequence c : chars) {
			if(string.contains(c)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * <p>
	 * Split {@code string} into tokens based on spaces while keeping strings
	 * together if they occur between parenthesis. Note this function does not
	 * handled double quotes embedded in double quotes well. Therefore it is
	 * best to use single quotes for internally.
	 * </p>
	 * <p>
	 * <strong>Note</strong>: This function was adapted from
	 * http://stackoverflow.com/a/366532/1336833
	 * </p>
	 * 
	 * @param string
	 * @return a list of tokens
	 * @author http://stackoverflow.com/a/366532/1336833
	 */
	public static List<String> tokenizeKeepQuotes(String string) {
		List<String> matches = Lists.newArrayList();
		Pattern regex = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'");
		Matcher matcher = regex.matcher(string);
		while (matcher.find()) {
			if(matcher.group(1) != null) {
				// Add double-quoted string without the quotes
				matches.add(matcher.group(1));
			}
			else if(matcher.group(2) != null) {
				// Add single-quoted string without the quotes
				matches.add(matcher.group(2));
			}
			else {
				// Add unquoted word
				matches.add(matcher.group());
			}
		}
		return matches;
	}

	/**
	 * Return a string representation of {@code object}
	 * 
	 * @param object
	 * @return the object string
	 * @source
	 *         https://code.google.com/p/dump-to-string/source/browse/src/main/
	 *         java
	 *         /com/borntojava/dts/DumpToString.java?r=
	 *         d284e6f89a120b3d5239f73e159eb112b81d28a5
	 * @author Shanbo Li
	 */
	public static String toString(Object object) {
		Set<Field> fields = Sets.newLinkedHashSet();
		Field[] array;
		Class<?> clazz = object.getClass();
		while (clazz != null) {
			array = clazz.getDeclaredFields();
			for (Field f : array) {
				if(Modifier.isStatic(f.getModifiers())) { // don't print static
															// fields
					continue;
				}
				fields.add(f);
			}
			clazz = clazz.getSuperclass(); // get fields defined in superclass
		}
		StringBuilder sb = new StringBuilder();
		sb.append(object.getClass().getSimpleName()).append('{');

		boolean firstRound = true;

		for (Field field : fields) {
			if(!firstRound) {
				sb.append(", ");
			}
			firstRound = false;
			field.setAccessible(true);
			try {
				final Object fieldObj = field.get(object);
				final String value;
				if(null == fieldObj) {
					value = "null";
				}
				else if(fieldObj instanceof Object[]) {
					value = Arrays.toString((Object[]) fieldObj);
				}
				else {
					value = fieldObj.toString();
				}
				sb.append(field.getName()).append('=').append('\'')
						.append(value).append('\'');
			}
			catch (IllegalAccessException ignore) {
				// this should never happen
			}

		}

		sb.append('}');
		return sb.toString();
	}

	private Strings() {} // non-initializable

}
