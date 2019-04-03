package com.cinchapi.concourse.page;

/**
 * This is the base class and marker for any valid state in the {@link Page}
 * builder. Each {@link State} is passed the current {@link Page} and holds
 * a reference.
 * <p>
 * For the purposes of a builder, a {@link State} typically describes what was
 * most recently consumed.
 * </p>
 */
public abstract class State {

    /**
     * A reference to the {@link Page} that is being built.
     */
    protected final Page page;

    /**
     * Construct a new instance.
     *
     * @param page
     */
    protected State(Page page) {
        this.page = page;
    }

}

