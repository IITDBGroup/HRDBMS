package com.exascale.misc;

public interface BitmapSymmetricAlgorithm {
    /**
     * Compute a Boolean symmetric query.
     *
     * @param f   symmetric boolean function to be processed
     * @param out the result of the query
     * @param set the inputs
     */
    void symmetric(UpdateableBitmapFunction f, BitmapStorage out, CompressedBitSet... set);
}