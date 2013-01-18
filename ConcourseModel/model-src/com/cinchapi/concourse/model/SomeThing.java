package com.cinchapi.concourse.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.cinchapi.concourse.annotations.Immutable;
import com.cinchapi.concourse.model.SomeRevision.Locators;
import com.cinchapi.concourse.model.id.SimpleIDService;
import com.cinchapi.concourse.model.id.IDService;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;

/**
 * Concourse implementation of the {@link Thing} interface.
 * 
 * @author jnelson
 */
@Immutable
public final class SomeThing implements Thing {

	/**
	 * Random and unique identifier.
	 */
	private final UnsignedLong id;

	/**
	 * Current snapshot of the metadata.
	 */
	private final Metadata metadata;

	/**
	 * Maps a <code>key</code> to a list of <code>locators</code>. Maintains all
	 * the distinct <code>values</code> mapped from
	 * the <code>key</code> by virtue of the locator.
	 */
	private final Map<String, List<String>> locators;

	/**
	 * Maps a <code>locator</code> to a list of <code>revisions</code>.
	 * Maintains all the instances of revisions involving a <code>key</code> and
	 * <code>value</code> by virtue of the locator.
	 */
	private final Map<String, List<Revision<?>>> revisions;

	private static int EXPECTED_NUM_KEYS = 1000; // TODO should be configurable?
	private static int EXPECTED_NUM_UNIQUE_PROPERTIES = 3000; // TODO should be
																// configurable
	private static IDService ids = new SimpleIDService();

	/**
	 * Construct and create a new {@link SomeThing} by specifying the
	 * <code>classifier</code> and <code>label</code>.
	 * 
	 * @param classifier
	 * @param label
	 */
	public SomeThing(String classifier, String label) {
		this(ids.requestRandom(),
				new SomeMetadata(classifier, label, DateTime.now()),
				new HashMap<String, List<String>>(EXPECTED_NUM_KEYS),
				new HashMap<String, List<Revision<?>>>(
						EXPECTED_NUM_UNIQUE_PROPERTIES));
	}

	/**
	 * Construct a {@link SomeThing} with the specified parameters.
	 * 
	 * @param id
	 * @param metadata
	 * @param locators
	 * @param revisions
	 */
	public SomeThing(
			UnsignedLong id,
			SomeMetadata metadata,
			Map<String, List<String>> locators,
			Map<String, List<Revision<?>>> revisions) {
		this.id = id;
		this.metadata = metadata;
		this.locators = locators;
		this.revisions = revisions;
	}

	@Override
	public <T> Revision<T> add(String key, T value){
		if(!this.contains(key, value)){
			SomeRevision<T> revision = new SomeRevision<T>(this, key,
					value);
			String locator = revision.getLocator();
			associate(key, locator);
			associate(locator, revision);
			return revision;
		}
		else{
			return null;
		}
	}

	@Override
	public <T> Revision<T> remove(String key, T value){
		if(this.contains(key, value)){
			SomeRevision<T> revision = new SomeRevision<T>(this, key,
					value);
			String locator = Locators.generate(id, key, value);
			associate(locator, revision);
			return revision;
		}
		else{
			return null;
		}
	}

	/**
	 * Associate a <code>locator</code> (and the encoded <code>value</code>)
	 * with a <code>key</code>, if necessary.
	 * 
	 * @param key
	 * @param locator
	 */
	private void associate(String key, String locator){
		List<String> keyLocators = locators.get(key);
		if(keyLocators == null){
			keyLocators = Lists.newArrayList();
			locators.put(key, keyLocators);
		}
		if(!keyLocators.contains(locator)){
			keyLocators.add(locator);
		}
	}

	/**
	 * Associate a <code>revision</code> with a <code>locator</code>.
	 * 
	 * @param locator
	 * @param revision
	 */
	private <T> void associate(String locator, Revision<T> revision){
		List<Revision<?>> locatorRevisions = revisions.get(locator);
		if(locatorRevisions == null){
			locatorRevisions = Lists.newArrayList();
			revisions.put(locator, locatorRevisions);
		}
		locatorRevisions.add(revision);
	}

	/**
	 * Return <code>true</code> if there are an odd number of revisions
	 * involving the <code>key</code> to <code>value</code> mapping (via the
	 * locator), otherwise
	 * 
	 * @return <code>false</code>.
	 */
	@Override
	public <T> boolean contains(String key, T value){
		String locator = Locators.generate(id, key, value);
		return this.containsEncodedProperty(locator);
	}

	/**
	 * Determine if the <code>locator</code> represents a <code>key</code> to
	 * <code>value</code> mapping that currently
	 * exists.
	 * 
	 * @param locator
	 * @return <code>true</code> if there are an odd number of revisions mapped
	 *         from the <code>locator</code>, otherwise <code>false</code>.
	 */
	private boolean containsEncodedProperty(String locator){
		return revisions.get(locator).size() % 2 != 0;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<?> get(String key){
		List values = Lists.newArrayList();
		for(String locator : locators.get(key)){
			if(this.containsEncodedProperty(locator)){
				Revision<?> lastRevision = revisions.get(locator).get(
						revisions.get(locator).size() - 1);
				values.add(lastRevision.getValue());
			}
		}
		return values;
	}

	@Override
	public UnsignedLong getId(){
		return id;
	}

	@Override
	public String getClassifier(){
		return metadata.getClassifier();
	}

	@Override
	public String getLabel(){
		return metadata.getLabel();
	}

	/**
	 * Set the associated <code>label</code>.
	 * 
	 * @param label
	 */
	public void setLabel(String label){
		((SomeMetadata) metadata).setLabel(label);
	}

	/**
	 * Returns a current snapshot of the contained keys, changes to the
	 * underlying data are not tracked in the returned list.
	 */
	@Override
	public List<String> getKeys(){
		return Lists.newArrayList(locators.keySet());
	}

	/**
	 * Delete this thing.
	 */
	public void delete(){
		((SomeMetadata) metadata).setDeleted(DateTime.now());
	}

	/**
	 * Return <code>true</code> if this thing has been deleted.
	 * 
	 * @return <code>true</code> if <code>this.metadata.getDeletedTime()</code>
	 *         is <code>null</code>.
	 */
	public boolean isDeleted(){
		return metadata.getDeleted() != null;
	}

	@Override
	public int hashCode(){
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj){
		if(this == obj) return true;
		if(obj == null) return false;
		if(getClass() != obj.getClass()) return false;
		SomeThing other = (SomeThing) obj;
		if(id == null){
			if(other.id != null) return false;
		}
		else if(!id.equals(other.id)) return false;
		return true;
	}

}
