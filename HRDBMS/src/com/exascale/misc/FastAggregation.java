package com.exascale.misc;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/*
 * Copyright 2009-2016, Daniel Lemire, Cliff Moon, David McIntosh, Robert Becho, Google Inc., Veronika Zenz, Owen Kaser, Gregory Ssi-Yan-Kai, Rory Graves
 * Licensed under the Apache License, Version 2.0.
 */

/**
 * Fast algorithms to aggregate many bitmaps. These algorithms are just given as
 * reference. They may not be faster than the corresponding methods in the
 * CompressedBitSet class.
 *
 * @author Daniel Lemire
 */
public final class FastAggregation {

    /** Private constructor to prevent instantiation */
    private FastAggregation() {}

    /**
     * Compute the and aggregate using a temporary uncompressed bitmap.
     *
     * This function does not seek to match the "sizeinbits" attributes
     * of the input bitmaps.
     *
     * @param bitmaps the source bitmaps
     * @param bufSize buffer size used during the computation in 64-bit
     *                words (per input bitmap)
     * @return the or aggregate.
     */
    public static CompressedBitSet bufferedand(final int bufSize,
                                                   final CompressedBitSet... bitmaps) {
        CompressedBitSet answer = new CompressedBitSet();
        bufferedandWithContainer(answer, bufSize, bitmaps);
        return answer;
    }

    /**
     * Compute the and aggregate using a temporary uncompressed bitmap.
     * 
     * This function does not seek to match the "sizeinbits" attributes
     * of the input bitmaps.
     *
     * @param container where the aggregate is written
     * @param bufSize   buffer size used during the computation in 64-bit
     *                  words (per input bitmap)
     * @param bitmaps   the source bitmaps
     */
    public static void bufferedandWithContainer(
            final BitmapStorage container, final int bufSize,
            final CompressedBitSet... bitmaps) {

        java.util.LinkedList<IteratingBufferedRunningLengthWord> al = new java.util.LinkedList<IteratingBufferedRunningLengthWord>();
        for (CompressedBitSet bitmap : bitmaps) {
            al.add(new IteratingBufferedRunningLengthWord(bitmap));
        }

        long[] hardbitmap = new long[bufSize * bitmaps.length];

        for (IteratingRLW i : al)
            if (i.size() == 0) {
                al.clear();
                break;
            }

        while (!al.isEmpty()) {
            Arrays.fill(hardbitmap, ~0l);
            long effective = Integer.MAX_VALUE;
            for (IteratingRLW i : al) {
                int eff = IteratorAggregation.inplaceand(
                        hardbitmap, i);
                if (eff < effective)
                    effective = eff;
            }
            for (int k = 0; k < effective; ++k)
                container.addWord(hardbitmap[k]);
            for (IteratingRLW i : al)
                if (i.size() == 0) {
                    al.clear();
                    break;
                }
        }
    }

    /**
     * Compute the or aggregate using a temporary uncompressed bitmap.
     *
     * @param bitmaps the source bitmaps
     * @param bufSize buffer size used during the computation in 64-bit
     *                words
     * @return the or aggregate.
     */
    public static CompressedBitSet bufferedor(final int bufSize,
                                                  final CompressedBitSet... bitmaps) {
        CompressedBitSet answer = new CompressedBitSet();
        bufferedorWithContainer(answer, bufSize, bitmaps);
        return answer;
    }

    /**
     * Compute the or aggregate using a temporary uncompressed bitmap.
     *
     * @param container where the aggregate is written
     * @param bufSize   buffer size used during the computation in 64-bit
     *                  words
     * @param bitmaps   the source bitmaps
     */
    public static void bufferedorWithContainer(
            final BitmapStorage container, final int bufSize,
            final CompressedBitSet... bitmaps) {
        int range = 0;
        CompressedBitSet[] sbitmaps = bitmaps.clone();
        Arrays.sort(sbitmaps, new Comparator<CompressedBitSet>() {
            @Override
            public int compare(CompressedBitSet a,
                               CompressedBitSet b) {
                return b.sizeInBits() - a.sizeInBits();
            }
        });

        java.util.ArrayList<IteratingBufferedRunningLengthWord> al = new java.util.ArrayList<IteratingBufferedRunningLengthWord>();
        for (CompressedBitSet bitmap : sbitmaps) {
            if (bitmap.sizeInBits() > range)
                range = bitmap.sizeInBits();
            al.add(new IteratingBufferedRunningLengthWord(bitmap));
        }
        long[] hardbitmap = new long[bufSize];
        int maxr = al.size();
        while (maxr > 0) {
            long effective = 0;
            for (int k = 0; k < maxr; ++k) {
                if (al.get(k).size() > 0) {
                    int eff = IteratorAggregation
                            .inplaceor(hardbitmap,
                                    al.get(k));
                    if (eff > effective)
                        effective = eff;
                } else
                    maxr = k;
            }
            for (int k = 0; k < effective; ++k)
                container.addWord(hardbitmap[k]);
            Arrays.fill(hardbitmap, 0);

        }
        container.setSizeInBitsWithinLastWord(range);
    }

    /**
     * Compute the xor aggregate using a temporary uncompressed bitmap.
     *
     * @param bitmaps the source bitmaps
     * @param bufSize buffer size used during the computation in 64-bit
     *                words
     * @return the xor aggregate.
     */
    public static CompressedBitSet bufferedxor(final int bufSize,
                                                   final CompressedBitSet... bitmaps) {
        CompressedBitSet answer = new CompressedBitSet();
        bufferedxorWithContainer(answer, bufSize, bitmaps);
        return answer;
    }

    /**
     * Compute the xor aggregate using a temporary uncompressed bitmap.
     * 
     *
     * @param container where the aggregate is written
     * @param bufSize   buffer size used during the computation in 64-bit
     *                  words
     * @param bitmaps   the source bitmaps
     */
    public static void bufferedxorWithContainer(
            final BitmapStorage container, final int bufSize,
            final CompressedBitSet... bitmaps) {
        int range = 0;
        CompressedBitSet[] sbitmaps = bitmaps.clone();
        Arrays.sort(sbitmaps, new Comparator<CompressedBitSet>() {
            @Override
            public int compare(CompressedBitSet a,
                               CompressedBitSet b) {
                return b.sizeInBits() - a.sizeInBits();
            }
        });

        java.util.ArrayList<IteratingBufferedRunningLengthWord> al = new java.util.ArrayList<IteratingBufferedRunningLengthWord>();
        for (CompressedBitSet bitmap : sbitmaps) {
            if (bitmap.sizeInBits() > range)
                range = bitmap.sizeInBits();
            al.add(new IteratingBufferedRunningLengthWord(bitmap));
        }
        long[] hardbitmap = new long[bufSize];
        int maxr = al.size();
        while (maxr > 0) {
            long effective = 0;
            for (int k = 0; k < maxr; ++k) {
                if (al.get(k).size() > 0) {
                    int eff = IteratorAggregation.inplacexor(hardbitmap, al.get(k));
                    if (eff > effective)
                        effective = eff;
                } else
                    maxr = k;
            }
            for (int k = 0; k < effective; ++k)
                container.addWord(hardbitmap[k]);
            Arrays.fill(hardbitmap, 0);
        }
        container.setSizeInBitsWithinLastWord(range);
    }

    /**
     * Uses a priority queue to compute the or aggregate.
     * 
     * This algorithm runs in linearithmic time (O(n log n)) with respect to the number of bitmaps.
     *
     * @param <T>     a class extending LogicalElement (like a compressed
     *                bitmap)
     * @param bitmaps bitmaps to be aggregated
     * @return the or aggregate
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T extends LogicalElement> T or(T... bitmaps) {
        PriorityQueue<T> pq = new PriorityQueue<T>(bitmaps.length,
                new Comparator<T>() {
                    @Override
                    public int compare(T a, T b) {
                        return a.sizeInBytes()
                                - b.sizeInBytes();
                    }
                }
        );
        Collections.addAll(pq, bitmaps);
        while (pq.size() > 1) {
            T x1 = pq.poll();
            T x2 = pq.poll();
            pq.add((T) x1.or(x2));
        }
        return pq.poll();
    }

    /**
     * Uses a priority queue to compute the or aggregate.
     * 
     * The content of the container is overwritten.
     * 
     * This algorithm runs in linearithmic time (O(n log n)) with respect to the number of bitmaps.
     *
     * @param container where we write the result
     * @param bitmaps   to be aggregated
     */
    public static void orToContainer(final BitmapStorage container,
                                     final CompressedBitSet... bitmaps) {
        if (bitmaps.length < 2)
            throw new IllegalArgumentException(
                    "We need at least two bitmaps");
        PriorityQueue<CompressedBitSet> pq = new PriorityQueue<CompressedBitSet>(
                bitmaps.length, new Comparator<CompressedBitSet>() {
            @Override
            public int compare(CompressedBitSet a,
                               CompressedBitSet b) {
                return a.sizeInBytes()
                        - b.sizeInBytes();
            }
        }
        );
        Collections.addAll(pq, bitmaps);
        while (pq.size() > 2) {
            CompressedBitSet x1 = pq.poll();
            CompressedBitSet x2 = pq.poll();
            pq.add(x1.or(x2));
        }
        pq.poll().orToContainer(pq.poll(), container);
    }
    
    /**
     * Simple algorithm that computes the OR aggregate.
     * 
     * @param bitmaps input bitmaps
     * @return new bitmap containing the aggregate
     */
    public static CompressedBitSet or(final CompressedBitSet... bitmaps) {
        PriorityQueue<CompressedBitSet> pq = new PriorityQueue<CompressedBitSet>(bitmaps.length,
                new Comparator<CompressedBitSet>() {
                    @Override
                    public int compare(CompressedBitSet a, CompressedBitSet b) {
                        return a.sizeInBytes()
                                - b.sizeInBytes();
                    }
                }
        );
        Collections.addAll(pq, bitmaps);
        if(pq.isEmpty()) return new CompressedBitSet();
        while (pq.size() > 1) {
            CompressedBitSet x1 = pq.poll();
            CompressedBitSet x2 = pq.poll();
            pq.add(x1.or(x2));
        }
        return pq.poll();
    }
    
    /**
     * Simple algorithm that computes the XOR aggregate.
     * 
     * @param bitmaps input bitmaps
     * @return new bitmap containing the aggregate
     */
    public static CompressedBitSet xor(final CompressedBitSet... bitmaps) {
        PriorityQueue<CompressedBitSet> pq = new PriorityQueue<CompressedBitSet>(bitmaps.length,
                new Comparator<CompressedBitSet>() {
                    @Override
                    public int compare(CompressedBitSet a, CompressedBitSet b) {
                        return a.sizeInBytes()
                                - b.sizeInBytes();
                    }
                }
        );
        Collections.addAll(pq, bitmaps);
        if(pq.isEmpty()) return new CompressedBitSet();
        while (pq.size() > 1) {
            CompressedBitSet x1 = pq.poll();
            CompressedBitSet x2 = pq.poll();
            pq.add(x1.xor(x2));
        }
        return pq.poll();
    }
    
    /**
     * Simple algorithm that computes the OR aggregate.
     * 
     * @param bitmaps input bitmaps
     * @return new bitmap containing the aggregate
     */
    public static CompressedBitSet or(final Iterator<CompressedBitSet> bitmaps) {
        PriorityQueue<CompressedBitSet> pq = new PriorityQueue<CompressedBitSet>(32,
                new Comparator<CompressedBitSet>() {
                    @Override
                    public int compare(CompressedBitSet a, CompressedBitSet b) {
                        return a.sizeInBytes()
                                - b.sizeInBytes();
                    }
                }
        );
        while(bitmaps.hasNext())
            pq.add(bitmaps.next());
        if(pq.isEmpty()) return new CompressedBitSet();
        while (pq.size() > 1) {
            CompressedBitSet x1 = pq.poll();
            CompressedBitSet x2 = pq.poll();
            pq.add(x1.or(x2));
        }
        return pq.poll();
    }
    
    /**
     * Simple algorithm that computes the XOR aggregate.
     * 
     * @param bitmaps input bitmaps
     * @return new bitmap containing the aggregate
     */
    public static CompressedBitSet xor(final Iterator<CompressedBitSet> bitmaps) {
        PriorityQueue<CompressedBitSet> pq = new PriorityQueue<CompressedBitSet>(32,
                new Comparator<CompressedBitSet>() {
                    @Override
                    public int compare(CompressedBitSet a, CompressedBitSet b) {
                        return a.sizeInBytes()
                                - b.sizeInBytes();
                    }
                }
        );
        while(bitmaps.hasNext())
            pq.add(bitmaps.next());
        if(pq.isEmpty()) return new CompressedBitSet();
        while (pq.size() > 1) {
            CompressedBitSet x1 = pq.poll();
            CompressedBitSet x2 = pq.poll();
            pq.add(x1.xor(x2));
        }
        return pq.poll();
    }


    /**
     * Uses a priority queue to compute the xor aggregate.
     * 
     * This algorithm runs in linearithmic time (O(n log n)) with respect to the number of bitmaps.
     *
     * @param <T>     a class extending LogicalElement (like a compressed
     *                bitmap)
     * @param bitmaps bitmaps to be aggregated
     * @return the xor aggregate
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T extends LogicalElement> T xor(T... bitmaps) {
        PriorityQueue<T> pq = new PriorityQueue<T>(bitmaps.length,
                new Comparator<T>() {

                    @Override
                    public int compare(T a, T b) {
                        return a.sizeInBytes()
                                - b.sizeInBytes();
                    }
                }
        );
        Collections.addAll(pq, bitmaps);
        while (pq.size() > 1) {
            T x1 = pq.poll();
            T x2 = pq.poll();
            pq.add((T) x1.xor(x2));
        }
        return pq.poll();
    }

    /**
     * Uses a priority queue to compute the xor aggregate.
     * 
     * The content of the container is overwritten.
     * 
     * This algorithm runs in linearithmic time (O(n log n)) with respect to the number of bitmaps.
     *
     * @param container where we write the result
     * @param bitmaps   to be aggregated
     */
    public static void xorToContainer(final BitmapStorage container,
                                      final CompressedBitSet... bitmaps) {
        if (bitmaps.length < 2)
            throw new IllegalArgumentException(
                    "We need at least two bitmaps");
        PriorityQueue<CompressedBitSet> pq = new PriorityQueue<CompressedBitSet>(
                bitmaps.length, new Comparator<CompressedBitSet>() {
            @Override
            public int compare(CompressedBitSet a,
                               CompressedBitSet b) {
                return a.sizeInBytes()
                        - b.sizeInBytes();
            }
        }
        );
        Collections.addAll(pq, bitmaps);
        while (pq.size() > 2) {
            CompressedBitSet x1 = pq.poll();
            CompressedBitSet x2 = pq.poll();
            pq.add(x1.xor(x2));
        }
        pq.poll().xorToContainer(pq.poll(), container);
    }

}