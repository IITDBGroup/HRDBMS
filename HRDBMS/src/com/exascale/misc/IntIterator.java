package com.exascale.misc;

public interface IntIterator {

    /**
     * Is there more?
     *
     * @return true, if there is more, false otherwise
     */
    boolean hasNext();

    /**
     * Return the next integer
     *
     * @return the integer
     */
    int next();
}