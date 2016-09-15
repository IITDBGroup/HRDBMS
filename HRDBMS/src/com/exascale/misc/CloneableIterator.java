package com.exascale.misc;

public interface CloneableIterator<E> extends Cloneable {

    /**
     * @return whether there is more
     */
    boolean hasNext();

    /**
     * @return the next element
     */
    E next();

    /**
     * @return a copy
     * @throws CloneNotSupportedException this should never happen in practice
     */
    CloneableIterator<E> clone() throws CloneNotSupportedException;

}