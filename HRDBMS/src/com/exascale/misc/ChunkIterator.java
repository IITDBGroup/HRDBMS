package com.exascale.misc;

public interface ChunkIterator {

    /**
     * Is there more?
     *
     * @return true, if there is more, false otherwise
     */
    boolean hasNext();

    /**
     * Return the next bit
     *
     * @return the bit
     */
    boolean nextBit();

    /**
     * Return the length of the next bit
     *
     * @return the length
     */
    int nextLength();

    /**
     * Move the iterator at the next different bit
     */
    void move();

    /**
     * Move the iterator at the next ith bit
     *
     * @param bits  the number of bits to skip
     */
    void move(int bits);

}