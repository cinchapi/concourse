package com.cinchapi.concourse.plugin.data;

public class DatasetEntry<E, A, V> {
    
    private E entity;
    private A attribute;
    private V value;
    
    public DatasetEntry(E entity, A attribute, V value) {
        this.entity = entity;
        this.attribute = attribute;
        this.value = value;
    }
    
    public E entity() {
        return entity;
    }
    
    public A attribute() {
        return attribute;
    }
    
    public V value() {
        return value;
    }

}
