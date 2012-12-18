package com.cinchapi.concourse.model.simple;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import static com.cinchapi.concourse.Constants.*;

import org.joda.time.DateTime;

import com.cinchapi.concourse.id.LongId;
import com.cinchapi.concourse.id.LongIdGenerator;
import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.MetaProperty;
import com.cinchapi.concourse.model.Metadata;
import com.cinchapi.concourse.model.Property;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * A simple implementation of the {@link Entity} interface.
 * @author jnelson
 *
 */
public class SimpleEntity implements Entity{

	/**
	 * A collection mapping a String, <code>key</code> to a set of {@link SimplePropertyRecord}s where each record
	 * associates this <code>entity</code> to a <code>property</code> where <code>property.getKey() == key</code>.
	 */
	protected Map<String,Set<SimplePropertyRecord>> data;
	protected LongId id; 
	protected SimpleMetadata metadata;
	
	private static final LongIdGenerator ids = new LongIdGenerator();
	private static final Gson json;
	
	/**
	 * Create a new {@link SimpleEntity} with the <code>classifier</code> and <code>title</code>.
	 * @param classifier
	 * @param title
	 */
	public SimpleEntity(String classifier, String title){
		this.id = ids.requestId();
		this.metadata = new SimpleMetadata(this, classifier, title);
		this.data = new HashMap<String, Set<SimplePropertyRecord>>(DEFAULT_ENTITY_PROPERTIES_INIT_CAPACITY);
	}
	
	protected SimpleEntity() {}
	
	@SuppressWarnings("rawtypes")
	@Override
	public  SimplePropertyRecord add(Property property){
		SimplePropertyRecord record = new SimplePropertyRecord(this, property);
		String key = record.getProperty().getKey();
		Set<SimplePropertyRecord> records;
		if(data.containsKey(key)){
			records = data.get(key);
		}
		else{
			records = new HashSet<SimplePropertyRecord>();
			data.put(key, records);
		}
		return records.add(record) ? record: null;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public  boolean contains(Property property){
		return get(property.getKey()).contains(property);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public  Set<Property> get(String key){
		Set<Property> properties = new HashSet<Property>();
		Iterator<SimplePropertyRecord> it = data.get(key).iterator();
		while(it.hasNext()){
			SimplePropertyRecord record = it.next();
			if(!record.isMarkAsRemoved()){
				properties.add(record.getProperty());
			}
		}
		return properties;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public  SimplePropertyRecord remove(Property property){
		SimplePropertyRecord record = new SimplePropertyRecord(this, property);
		String key = record.getProperty().getKey();
		Iterator<SimplePropertyRecord> it = data.get(key).iterator();
		while(it.hasNext()){
			SimplePropertyRecord stored = it.next();
			if(stored.equals(record)){
				stored.markAsRemoved();
				return stored;
			}
		}
		return null;
	}
	
	@Override
	public boolean setTitle(String title) {
		metadata.set(SimpleMetadata.TITLE_KEY, title);
		return metadata.get(SimpleMetadata.TITLE_KEY).equals(title);
	}
	
	@Override
	public LongId getId(){
		return id;
	}

	@Override
	public Metadata getMetadata() {
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
	
	static{
		/*
		 * Custom JsonSerializers
		 * https://sites.google.com/site/gson/gson-user-guide#TOC-Custom-Serialization-and-Deserialization
		 */
		json = new GsonBuilder().registerTypeHierarchyAdapter(SimpleEntity.class, new JsonSerializer<SimpleEntity>(){

			@Override
			@SuppressWarnings("rawtypes")
			public JsonElement serialize(SimpleEntity src, Type typeOfSrc, JsonSerializationContext context) {
				JsonObject object = new JsonObject();
				object.addProperty("id", src.getId().toString());
				
				Iterator<MetaProperty> metaProperties = src.getMetadata().iterator();
				JsonObject metadata = new JsonObject();
				while(metaProperties.hasNext()){
					MetaProperty<?> property = metaProperties.next();
					Object value;
					if(property.getKey().equalsIgnoreCase(SimpleMetadata.CREATED_KEY)){
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
					Set<Property> propertySet = src.get(key);
					
					JsonElement value;					
					Iterator<Property> propertySetIterator = propertySet.iterator();
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
