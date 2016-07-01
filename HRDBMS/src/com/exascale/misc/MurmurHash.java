package com.exascale.misc;

import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

public final class MurmurHash
{
	private static XXHash64 hasher = XXHashFactory.fastestJavaInstance().hash64();

	public static long hash64(final byte[] data, final int length)
	{
		return hasher.hash(data, 0, length, 0xc6a4a7935bd1e995L);
	}

	/*
	 * MurmurHash V2 public static long hash64(final byte[] data, final int
	 * length) { final long seed = 0xe17a1465l; final long m =
	 * 0xc6a4a7935bd1e995L; final int r = 47;
	 *
	 * long h = seed ^ (length * m);
	 *
	 * // final int length8 = length >> 3; final int test = length - 8;
	 *
	 * for (int i = 0; i <= test; i += 8) { // final int i8 = i << 3; long k =
	 * ((long)data[i + 0] & 0xff) + (((long)data[i + 1] & 0xff) << 8) +
	 * (((long)data[i + 2] & 0xff) << 16) + (((long)data[i + 3] & 0xff) << 24) +
	 * (((long)data[i + 4] & 0xff) << 32) + (((long)data[i + 5] & 0xff) << 40) +
	 * (((long)data[i + 6] & 0xff) << 48) + (((long)data[i + 7] & 0xff) << 56);
	 *
	 * k *= m; k ^= k >>> r; k *= m;
	 *
	 * h ^= k; h *= m; }
	 *
	 * switch (length & 7) { case 7: h ^= (long)(data[(length & ~7) + 6] & 0xff)
	 * << 48; case 6: h ^= (long)(data[(length & ~7) + 5] & 0xff) << 40; case 5:
	 * h ^= (long)(data[(length & ~7) + 4] & 0xff) << 32; case 4: h ^=
	 * (long)(data[(length & ~7) + 3] & 0xff) << 24; case 3: h ^=
	 * (long)(data[(length & ~7) + 2] & 0xff) << 16; case 2: h ^=
	 * (long)(data[(length & ~7) + 1] & 0xff) << 8; case 1: h ^= data[length &
	 * ~7] & 0xff; h *= m; } ;
	 *
	 * h ^= h >>> r; h *= m; h ^= h >>> r;
	 *
	 * return h; }
	 */
}