package com.cinchapi.concourse.model;

import java.util.List;

import com.google.common.primitives.UnsignedLong;

/**
 * <p>
 * A uniquely identifiable representation of a self-contained element (i.e.
 * person, place, event, document, etc) that is describable by its metadata and
 * revision history.
 * </p>
 * <p>
 * <h2>Background</h2> Traditional RDBMS and even some NoSQL databases force
 * developers to structure data in unnatural ways, usually with tables or
 * columns. But in reality, humans see elements of the world basically as
 * things‒-things that have attributes and relationships with other things.
 * Different kinds of things exist. Things are related to other things in
 * various ways. And things both share attributes and have uniquely identifying
 * ones as well. Nevertheless, all data can be described in terms of things and
 * this generalization is the only assumption in the data model.
 * </p>
 * <p>
 * <h2>Data Model</h2>
 * <ul>
 * <li>{@link Metadata} is used to collect the intrinsic attributes.</li>
 * <li>A {@link Revision} list describes the history of <code>key</code> to
 * <code>value</code> mappings that have been added and removed. A key can map
 * to multiple values and multiple types (the only restriction is that a key
 * cannot map to a duplicate value/type pair).
 * </ul>
 * </p>
 * <p>
 * </p>
 * 
 * @author jnelson
 */
public interface Thing {

	/**
	 * Add, if possible, a mapping from <code>key</code> to <code>value</code>.
	 * 
	 * @param key
	 * @param value
	 * @return a new {@link Revision} or <code>null</code> if the
	 *         <code>property</code> cannot be added.
	 */
	public <T> Revision<T> add(String key, T value);

	/**
	 * Return <code>true</code> if <code>key</code> maps to <code>value</code>.
	 * 
	 * @param key
	 * @param value
	 * @return <code>true</code> if <code>this.add(key, value)</code> returns
	 *         <code>null</code>.
	 */
	public <T> boolean contains(String key, T value);

	/**
	 * Return the list of <code>values</code> mapped from <code>key</code>
	 * 
	 * @param key
	 * @return the <code>values</code>.
	 */
	public List<?> get(String key);

	/**
	 * Return the associated <code>id</code>.
	 * 
	 * @return the <code>id</code>.
	 */
	public UnsignedLong getId();

	/**
	 * Return the <code>classifier</code> attribute from the
	 * <code>metadata</code>.
	 * 
	 * @return the <code>classifier</code>.
	 */
	public String getClassifier();

	/**
	 * Return the <code>label</code> attribute from the <code>metadata</code>.
	 * 
	 * @return the <code>label</code>.
	 */
	public String getLabel();

	/**
	 * Return a list of the contained keys. Use {@link #get(String)} to
	 * get the values mapped from each <code>key</code>.
	 * 
	 * @return the <code>keys</code>.
	 */
	public List<String> getKeys();

	/**
	 * Remove the existing mapping from <code>key</code> to <code>value</code>.
	 * 
	 * @param key
	 * @param value
	 * @return a new {@link Revision} or <code>null</code> if the
	 *         <code>property</code> cannot be removed (i.e it is not
	 *         contained).
	 */
	public <T> Revision<T> remove(String key, T value);

	/**
	 * Two {@link Thing} objects are equal if and only if they have the same
	 * <code>id</code>.
	 * 
	 * @param obj
	 * @return <code>true</code> if <code>obj</code> is a {@link Thing} and
	 *         <code>this.getId().equals(obj.getId())</code> is
	 *         <code>true</code>.
	 */
	@Override
	public boolean equals(Object obj);

}
