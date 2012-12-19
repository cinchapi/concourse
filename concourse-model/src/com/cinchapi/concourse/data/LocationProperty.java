package com.cinchapi.concourse.data;

import com.cinchapi.concourse.annotations.DataType;
import com.javadocmd.simplelatlng.LatLng;

/**
 * A GIS {@link AbstractProperty} defined by a latitude and longitude.
 * @author jnelson
 *
 */
@DataType("location")
public class LocationProperty extends AbstractProperty<LatLng>{

	/**
	 * Create a new location Property.
	 * @param key
	 * @param value
	 */
	public LocationProperty(String key, LatLng value) {
		super(key, value);
	}
	
	/**
	 * Create a new location Property by using the latitude and longitude directly
	 * @param key
	 * @param latitude
	 * @param longitude
	 */
	public LocationProperty(String key, double latitude, double longitude){
		super(key, new LatLng(latitude, longitude));
	}

}
