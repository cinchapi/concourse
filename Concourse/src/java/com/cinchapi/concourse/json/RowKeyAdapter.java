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
package com.cinchapi.concourse.json;

import java.io.IOException;

import com.cinchapi.concourse.db.Key;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

/**
 * Enables the proper reading and writing of an {@link Key}.
 * 
 * @author jnelson
 */
public class RowKeyAdapter extends TypeAdapter<Key> {
	
	private static final String prepend = "{";
	private static final String append = "}";

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.google.gson.TypeAdapter#read(com.google.gson.stream.JsonReader)
	 */
	@Override
	public Key read(JsonReader in) throws IOException {
		if(in.peek() == JsonToken.NULL){
			in.nextNull();
			return null;
		}
		else{
			String json = in.nextString();
			String value = json.substring(prepend.length(), json.length()-(append.length()-1));
			return Key.create(Long.valueOf(value));
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.google.gson.TypeAdapter#write(com.google.gson.stream.JsonWriter,
	 * java.lang.Object)
	 */
	@Override
	public void write(JsonWriter out, Key value) throws IOException {
		if(value == null){
			out.nullValue();
		}
		else{
			String json = prepend+value.toString()+append;
			out.value(json);
		}

	}

}
