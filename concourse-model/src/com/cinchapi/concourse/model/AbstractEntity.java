package com.cinchapi.concourse.model;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import static com.cinchapi.concourse.model.MetadataRecord.TITLE_KEY;
import static com.cinchapi.concourse.model.MetadataRecord.CREATED_KEY;

import org.joda.time.DateTime;

import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.concourse.data.Property;
import com.cinchapi.concourse.id.Id;
import com.cinchapi.concourse.metadata.IntrinsicProperty;
import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.MetadataRecord;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Abstract implementation of the {@link Entity} interface.
 * @author jnelson
 *
 */
public abstract class AbstractEntity implements Entity{

	/**
	 * A collection mapping a String, <code>key</code> to a set of {@link PropertyRecord} objects where each record
	 * associates this <code>entity</code> to a <code>property</code> where <code>property.getKey() == key</code>.
	 */
	protected Map<String,Set<PropertyRecord<?>>> data;
	protected Id id; 
	protected MetadataRecord metadata;
	
	private static final Gson json;
	
	@NoDocumentation
	public AbstractEntity(String classifier, String title){
		this.id = createId();
		this.metadata = createMetadata(classifier, title);
		this.data = createData();
	}
	
	@NoDocumentation
	protected AbstractEntity() {}
	
	@Override
	public PropertyRecord<?> add(Property<?> property){
		PropertyRecord<?> record = createPropertyRecord(property);
		String key = record.getProperty().getKey();
		Set<PropertyRecord<?>> records;
		if(data.containsKey(key)){
			records = data.get(key);
		}
		else{
			records = createEmptyPropertyRecordSet();
			data.put(key, records);
		}
		return records.add(record) ? record: null;
	}
	
	@Override
	public boolean contains(Property<?> property){
		return get(property.getKey()).contains(property);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public Set<Property<?>> get(String key){
		Set<Property<?>> properties = createEmptyPropertySet();
		Iterator<PropertyRecord<?>> it = data.get(key).iterator();
		while(it.hasNext()){
			PropertyRecord record = it.next();
			if(!record.isMarkAsRemoved()){
				properties.add(record.getProperty());
			}
		}
		return properties;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public PropertyRecord<?> remove(Property<?> property){
		PropertyRecord record = createPropertyRecord(property);
		String key = record.getProperty().getKey();
		Iterator<PropertyRecord<?>> it = data.get(key).iterator();
		while(it.hasNext()){
			PropertyRecord stored = it.next();
			if(stored.equals(record)){
				stored.markAsRemoved();
				return stored;
			}
		}
		return null;
	}
	
	@Override
	public boolean setTitle(String title) {
		metadata.set(TITLE_KEY, title);
		return metadata.get(TITLE_KEY).equals(title);
	}
	
	@Override
	public Id getId(){
		return id;
	}

	@Override
	public MetadataRecord getMetadata() {
		return metadata;
	}
	
	@Override
	public Iterator<String> iterator() {
		return data.keySet().iterator();
	}
	
	@Override
	public String toString(){
		/*
		 * Need to pass base class to the serializer
		 * https://groups.google.com/forum/?fromgroups=#!topic/google-gson/0kp_85nAE6k
		 */
		return json.toJson(this, Entity.class);
	}
	
	/**
	 * Create the {@link Id} for a newly constructed <code>entity</code>.
	 * @return the <code>id</code>.
	 */
	protected abstract Id createId();
	
	/**
	 * Create the {@link MetadataRecord} for a newly constructed <code>entity</code>.
	 * @param classifier
	 * @param title
	 * @return
	 */
	protected abstract MetadataRecord createMetadata(String classifier, String title);
	
	/**
	 * Create the initial {@link #data} map for a newly constructed <code>entity</code>. 
	 * @return
	 */
	protected abstract Map<String, Set<PropertyRecord<?>>> createData();
	
	/**
	 * Create a {@link PropertyRecord} for the specified {@link Property}.
	 * @param property
	 * @return a new <code>property record</code>/
	 */
	protected abstract PropertyRecord<?> createPropertyRecord(Property<?> property);
	
	/**
	 * Create an empty {@link PropertyRecord} set.
	 * @return an empty set.
	 */
	protected abstract Set<PropertyRecord<?>> createEmptyPropertyRecordSet();

	/**
	 * Create an empty {@link Property} set.
	 * @return an empty set.
	 */
	protected abstract Set<Property<?>> createEmptyPropertySet();
	
	static{
		/*
		 * Custom JsonSerializers
		 * https://sites.google.com/site/gson/gson-user-guide#TOC-Custom-Serialization-and-Deserialization
		 */
		json = new GsonBuilder().registerTypeHierarchyAdapter(AbstractEntity.class, new JsonSerializer<AbstractEntity>(){

			@Override
			@SuppressWarnings("rawtypes")
			public JsonElement serialize(AbstractEntity src, Type typeOfSrc, JsonSerializationContext context) {
				JsonObject object = new JsonObject();
				object.addProperty("id", src.getId().toString());
				
				Iterator<IntrinsicProperty<?>> metaProperties = src.getMetadata().iterator();
				JsonObject metadata = new JsonObject();
				while(metaProperties.hasNext()){
					IntrinsicProperty<?> property = metaProperties.next();
					Object value;
					if(property.getKey().equalsIgnoreCase(CREATED_KEY)){
						value = ((DateTime)property.getValue()).getMillis();
					}
					else{
						value = property.getValue();
					}
					metadata.addProperty(property.getKey(), value.toString());
				}
				object.add("metadata", metadata);
				
				Iterator<String> keys = src.iterator();
				JsonObject data = new JsonObject();
				while(keys.hasNext()){
					String key = keys.next();
					Set<Property<?>> propertySet = src.get(key);
					
					JsonElement value;					
					Iterator<Property<?>> propertySetIterator = propertySet.iterator();
					if(propertySet.size() == 1){ 
						value = extractValue(propertySetIterator.next()); //no array necessary
					}
					else{ 
						value = new JsonArray();
						while(propertySetIterator.hasNext()){
							Property property = propertySetIterator.next();
							((JsonArray) value).add(extractValue(property));							
						}
					}
					data.add(key, value);
				}
				
				object.add("data", data);
				
				return object;
			}
			
			/**
			 * Extract the value from a {@link Property} and return a {@link JsonPrimitive}.
			 * @param property
			 * @return the {@link JsonPrimitive}
			 */
			@SuppressWarnings("rawtypes")
			public JsonPrimitive extractValue(Property property){
				JsonPrimitive value;
				if(property.getType().equalsIgnoreCase("long")){
					value = new JsonPrimitive((Number) property.getValue());
				}
				else{
					value = new JsonPrimitive(property.getValue().toString());
				}
				return value;
			}
			
		}).create();
	}
	
}
