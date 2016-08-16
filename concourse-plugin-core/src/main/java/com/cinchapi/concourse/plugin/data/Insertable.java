package com.cinchapi.concourse.plugin.data;

/**
 * <p>
 * An interface implemented by the {@link Dataset} that limits transactions to a
 * write-only (specifically insertion) basis. This prevents data leakage and
 * protects against malicious or uninformed usage.
 * </p>
 * 
 * <p>
 * The interface should be parameterized in the same way as the implementing
 * Dataset.
 * </p>
 *
 * @see {@link Dataset}
 * @param <E> entity
 * @param <A> attribute
 * @param <V> value
 */
public interface Insertable<E, A, V> {

    /**
     * Add an association between {@code attribute} and {@code value} within the
     * {@code entity}.
     * 
     * @param entity the entity
     * @param attribute the attribute
     * @param value the value
     * @return {@code true} if the association can be added because it didn't
     *         previously exist
     */
    public boolean insert(E entity, A attribute, V value);

}
