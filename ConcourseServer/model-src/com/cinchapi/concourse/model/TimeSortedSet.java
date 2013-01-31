package com.cinchapi.concourse.model;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedLong;

/**
 * A collection of objects that are sorted in reverse chronological order.
 * 
 * @author jnelson
 * @param <T> - the contained object type
 */
abstract class TimeSortedSet<T> implements Iterable<T> {

	@NotNull
	private Set<TimeSortableObject<T>> objects = Sets
			.newTreeSet(new Comparator<TimeSortableObject<T>>() {

				@Override
				public int compare(TimeSortableObject<T> o1,
						TimeSortableObject<T> o2){
					return o1.compareTo(o2);
				}

			});

	/**
	 * Add <code>object</code> to the set.
	 * 
	 * @param object
	 * @param timestamp
	 * @return this
	 */
	public TimeSortedSet<T> add(T object, UnsignedLong timestamp){
		objects.add(createObject(object, timestamp));
		return this;
	}

	/**
	 * Return <code>true</code> if <code>object</code> is contained in the
	 * set.
	 * 
	 * @param value
	 * @return <code>true</code> if <code>object</code> is present.
	 */
	public boolean contains(@NotNull Object object){
		return objects.contains(object);
	}

	/**
	 * Return the number of objects in the set.
	 * 
	 * @return the size.
	 */
	public int size(){
		return objects.size();
	}

	/**
	 * Return an iterator over the sorted set.
	 */
	@Override
	@NotNull
	public Iterator<T> iterator(){
		final Iterator<TimeSortableObject<T>> internal = objects
				.iterator();
		return new Iterator<T>() {

			@Override
			public boolean hasNext(){
				return internal.hasNext();
			}

			@Override
			public T next(){
				return internal.next().getObject();
			}

			@Override
			public void remove(){
				internal.remove();
			}
		};
	}
	
	@Override
	public int hashCode(){
		final int prime = 31;
		int result = 1;
		result = prime * result + ((objects == null) ? 0 : objects.hashCode());
		return result;
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean equals(Object obj){
		if(this == obj) return true;
		if(obj == null) return false;
		if(getClass() != obj.getClass()) return false;
		TimeSortedSet<T> other = (TimeSortedSet<T>) obj;
		if(objects == null){
			if(other.objects != null) return false;
		}
		else if(!objects.equals(other.objects)) return false;
		return true;
	}
	
	@Override
	public String toString(){
		return objects.toString();
	}
	

	/**
	 * Create and return a new {@link TimeSortableObject} based on the
	 * parameters.
	 * 
	 * @param object
	 * @param timestamp
	 * @return the new object
	 */
	protected abstract TimeSortableObject<T> createObject(T object,
			UnsignedLong timestamp);
	
	/**
	 * An object that is sortable in reverse chronological order.
	 * 
	 * @author jnelson
	 * @param <E> - the object type
	 */
	abstract class TimeSortableObject<E> implements
			Comparable<TimeSortableObject<E>> {

		private final E object;
		private final UnsignedLong timestamp;

		public TimeSortableObject(E object, UnsignedLong timestamp) {
			this.object = object;
			this.timestamp = timestamp;
		}

		/**
		 * Compares such that the objects with more recent timestamps come first.
		 */
		@Override
		public int compareTo(TimeSortableObject<E> o){
			return -1 * this.timestamp.compareTo(o.timestamp);
		}

		/**
		 * Return the encapsulated object.
		 * 
		 * @return the object.
		 */
		public E getObject(){
			return object;
		}
	}

}
