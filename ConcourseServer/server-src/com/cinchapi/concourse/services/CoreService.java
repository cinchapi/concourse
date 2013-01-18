package com.cinchapi.concourse.services;

import java.util.Set;

import com.cinchapi.concourse.annotations.Idempotent;
import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.Id;
import com.cinchapi.concourse.model.Property;

/**
 * Provides the core interactions with Concourse data.
 * 
 * @author jnelson
 */
public interface CoreService {

	/**
	 * Add a {@link Property} where the <code>key</code> maps to
	 * <code>value</code> for the {@link Entity} identified by <code>id</code>.
	 * 
	 * @param key
	 * @param value
	 * @param id
	 * @return <code>true</code> if the <code>property</code> is added.
	 */
	@Idempotent
	public boolean add(String key, Object value, Id id);

	/**
	 * Create a new {@link Entity} with the <code>classifier</code> and
	 * <code>title</code>.
	 * 
	 * @param classifier
	 * @param title
	 * @return the <code>entity</code> {@link Id}.
	 */
	public Id create(String classifier, String title);

	/**
	 * Delete the {@link Entity} identified by <code>id</code>.
	 * 
	 * @param id
	 * @return true if the <code>entity</code> is deleted.
	 */
	public boolean delete(Id id);

	/**
	 * Return <code>true</code> if there is an {@link Entity} identified by
	 * <code>id</code>.
	 * 
	 * @param id
	 * @return <code>true</code> if the <code>entity</code> exists.
	 */
	@Idempotent
	public boolean exists(Id id);

	/**
	 * Get the values mapped from <code>key</code> on the {@link Entity}
	 * identified by <code>id</code>.
	 * 
	 * @param key
	 * @param id
	 * @return the <code>value</code> set.
	 */
	@Idempotent
	public Set<Object> get(String key, Id id);

	/**
	 * List each {@link Entity} in the <code>classifier</code>.
	 * 
	 * @param classifier
	 * @return the <code>id</code> set.
	 */
	@Idempotent
	public Set<Id> list(String classifier);

	/**
	 * Remove the mapping of <code>key</code> to <code>value</code> for the
	 * {@link Entity} identified by <code>id</code>.
	 * 
	 * @param key
	 * @param value
	 * @param id
	 * @return <code>true</code> if the mapping is removed.
	 */
	public boolean remove(String key, Object value, Id id);

	/**
	 * Select each {@link Entity} in <code>classifier</code> that matches the
	 * <code>criteria</code>.
	 * 
	 * @param classifier
	 * @param criteria
	 * @return the <code>id</code> set.
	 */
	@Idempotent
	public Set<Id> select(String classifier, String criteria);

	/**
	 * Remove any existing <code>values</code> mapped from <code>key</code> and
	 * add a new {@link Property} where <code>key</code> maps to
	 * <code>value</code> on the {@link Entity} identified by <code>id</code>.
	 * 
	 * @param key
	 * @param value
	 * @param id
	 * @return <code>true</code> if the <code>property</code> is added.
	 */
	@Idempotent
	public boolean set(String key, Object value, Id id);

}
