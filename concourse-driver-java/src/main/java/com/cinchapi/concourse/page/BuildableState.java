package com.cinchapi.concourse.page;

/**
 * The base class for a Page state that can be transformed into a complete
 * and well-formed {@link Page}.
 */
public abstract class BuildableState extends State {

    /**
     * Construct a new instance.
     *
     * @param page
     */
    protected BuildableState(Page page) {
        super(page);
    }

    /**
     * Build and return the {@link Page}.
     *
     * @return the built Page
     */
    public final Page build() {
        page.close();
        return page;
    }
}
