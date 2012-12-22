package com.cinchapi.concourse.model;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.cinchapi.concourse.model.api.Metadata.CREATED_KEY;
import static com.cinchapi.concourse.model.api.Metadata.TITLE_KEY;

import org.joda.time.DateTime;

import com.cinchapi.concourse.id.Id;
import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.model.api.Metadata;
import com.cinchapi.concourse.model.api.Modification;
import com.cinchapi.concourse.property.api.IntrinsicProperty;
import com.cinchapi.concourse.property.api.Property;
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
	
	private final Id id; 
	private final Metadata metadata;
	private final Map<String, Modification<?>> modifications; 
	private final Map<String, Set<Property<?>>> data; 
	
	private static final Gson json;
	
	/* Non-Initializable */
	public AbstractEntity(String classifier, String title){
		id = createId();
		metadata = createMetadata(classifier, title);
		data = createEmptyData();
		modifications = createEmptyModifications();
	}
	
	@Override
	public Modification<?> add(Property<?> property){
		String key = property.getKey();
		
		Set<Property<?>> properties;
		if(data.containsKey(key)){
			properties = data.get(key);
		}
		else{
			properties = createEmptyPropertySet();
			data.put(key, properties);
		}
		
		Modification<?> mod = null;
		if(properties.add(property)){
			mod = createModification(property, Modification.Type.PROPERTY_ADDED);
			modifications.put(mod.getLookup(), mod);
		}
		return mod;
	}
	
	@Override
	public boolean contains(Property<?> property){
		return get(property.getKey()).contains(property);
	}
	
	@Override
	public Set<Property<?>> get(String key){
		return data.get(key);
	}
	
	@Override
	public Modification<?> remove(Property<?> property){
		return data.get(property.getKey()).remove(property) 
				? createModification(property, Modification.Type.PROPERTY_REMOVED) 
				: null;
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
	public Metadata getMetadata() {
		return metadata;
	}
	
	@Override
	public Iterator<String> keyIterator() {
		return data.keySet().iterator();
	}
	
	@Override
	public Iterator<Modification<?>> modificationIterator(){
		return modifications.values().iterator();
	}
	
	@Override
	public String toString(){
		/*
		 * Need to pass base class to the serializer
		 * https://groups.google.com/forum/?fromgroups=#!topic/google-gson/0kp_85nAE6k
		 */
		return json.toJson(this, AbstractEntity.class);
	}
	
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
				
				Iterator<String> keys = src.keyIterator();
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
	
	protected abstract Id createId();
	protected abstract Set<Property<?>> createEmptyPropertySet();
	protected abstract Modification<?> createModification(Property<?> property, Modification.Type type);
	protected abstract Metadata createMetadata(String classifier, String title);
	protected abstract Map<String, Modification<?>> createEmptyModifications();
	protected abstract Map<String, Set<Property<?>>> createEmptyData();
	
}
