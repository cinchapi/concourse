package com.cinchapi.concourse.model;

import org.joda.time.DateTime;

/**
 * Implementation of the {@link Metadata} interface for {@link SomeThing}
 * objects.
 * 
 * @author jnelson
 */
final class SomeMetadata implements Metadata {

	private String classifier;
	private String label;
	private DateTime created;
	private DateTime deleted;

	/**
	 * Construct new metadata with the specified values.
	 * 
	 * @param classifier
	 * @param label
	 * @param created
	 */
	SomeMetadata(String classifier, String label, DateTime created) {
		this(classifier, label, created, null);
	}

	/**
	 * Construct new metadata with the specified values.
	 * 
	 * @param classifier
	 * @param label
	 * @param created
	 * @param deleted
	 */
	SomeMetadata(String classifier, String label, DateTime created,
			DateTime deleted) {
		this.setClassifier(classifier);
		this.setLabel(label);
		this.setCreated(created);
		this.setDeleted(deleted);
	}

	@Override
	public String getClassifier(){
		return classifier;
	}

	/**
	 * Set the <code>classifier</code>.
	 * 
	 * @param classifier
	 */
	private void setClassifier(String classifier){
		if(classifier == null){
			throw new NullPointerException("The value cannot be null.");
		}
		this.classifier = classifier;
	}

	@Override
	public String getLabel(){
		return label;
	}

	/**
	 * Set the <code>label</code>.
	 * 
	 * @param label
	 */
	public void setLabel(String label){
		if(label == null){
			throw new NullPointerException("The value cannot be null.");
		}
		this.label = label;
	}

	@Override
	public DateTime getCreated(){
		return created;
	}

	/**
	 * Set the <code>created</code> timestamp
	 * 
	 * @param created
	 */
	private void setCreated(DateTime created){
		if(created == null){
			throw new NullPointerException("The value cannot be null.");
		}
		if(deleted != null && !created.isBefore(deleted)){
			throw new IllegalArgumentException(
					"The created timestamp must occur before the deleted timestamp.");
		}
		this.created = created;
	}

	@Override
	public DateTime getDeleted(){
		return deleted;
	}

	/**
	 * Set the <code>deleted</code> timestamp.
	 * 
	 * @param deleted
	 */
	public void setDeleted(DateTime deleted){
		if(!created.isBefore(deleted)){
			throw new IllegalArgumentException(
					"The deleted timestamp must occur after the deleted timestamp.");
		}
		this.deleted = deleted;
	}

}
