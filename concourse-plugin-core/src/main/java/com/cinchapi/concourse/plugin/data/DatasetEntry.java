package com.cinchapi.concourse.plugin.data;

/**
 * Container for objects to add into the {@link Dataset}.
 *
 * @param <E> the entity, typically a record in the form of a {@code Long}
 * @param <A> the attribute, typically a key in the form of a {@code String}
 * @param <V> the value, typically in the form of an {@code Object}
 */
public class DatasetEntry<E, A, V> {
    
    /**
     * Private instance variables
     */
    private E entity;
    private A attribute;
    private V value;
    
    /**
     * Constructs an instance of the entry.
     * @param entity the record for the entry
     * @param attribute the key for the entry
     * @param value the value for the entry
     */
    public DatasetEntry(E entity, A attribute, V value) {
        this.entity = entity;
        this.attribute = attribute;
        this.value = value;
    }
    
    /**
     * Returns the entity
     */
    public E entity() {
        return entity;
    }
    
    /**
     * Returns the attribute
     */
    public A attribute() {
        return attribute;
    }
    
    /**
     * Returns the value
     */
    public V value() {
        return value;
    }
    
    @Override
    public String toString() {
        return String.format("(%s, %s, %s)", entity, attribute, value);
    }

}
