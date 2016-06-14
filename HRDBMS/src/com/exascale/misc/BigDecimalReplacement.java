package com.exascale.misc;

import java.util.concurrent.locks.ReentrantLock;

public class BigDecimalReplacement
{
	private long[] data;
	private boolean pos;
	private short decPos;
	private volatile boolean queued = false;
	private volatile BigDecimalReplacement queue = null;
	private final ReentrantLock lock = new ReentrantLock();

	public BigDecimalReplacement(double val)
	{
		long bits = Double.doubleToLongBits(val);
		data = new long[1];
		data[0] = (bits & (0xFFFFFFFFFFFFFL)) + 0x10000000000000L;
		pos = (bits & 0x8000000000000000L) == 0;
		short exp = (short)(((bits & 0x7FF0000000000000L) >> 52) - 1023);
		decPos = (short)(52 - exp);
		if (decPos < 0)
		{
			underflow();
		}
		else if (decPos > 63)
		{
			overflow();
		}
	}

	private BigDecimalReplacement(long[] data, boolean pos, short decPos)
	{
		this.data = data;
		this.pos = pos;
		this.decPos = decPos;
	}

	public void add(BigDecimalReplacement val)
	{
		if (!lock.tryLock())
		{
			delayedAdd(val);
			return;
		}

		BigDecimalReplacement rhs = null;
		short resDecPos;
		if (this.decPos < val.decPos)
		{
			moveDec(val);
			rhs = val;
			resDecPos = val.decPos;
		}
		else if (val.decPos < this.decPos)
		{
			// rhs = moveDec(val, this);
			val.moveDec(this);
			rhs = val;
			resDecPos = this.decPos;
		}
		else
		{
			rhs = val;
			resDecPos = this.decPos;
		}

		short max = (short)Math.max(data.length, rhs.data.length);
		short min = (short)Math.min(data.length, rhs.data.length);
		boolean lhsMin = (data.length == min);
		int i = 0;
		short carry = 0;
		Boolean resultPos = null;
		boolean lhsPos = pos;
		boolean rhsPos = rhs.pos;
		if (lhsPos && rhsPos)
		{
			resultPos = true;
		}
		else if (!lhsPos && !rhsPos)
		{
			resultPos = false;
		}

		if (!lhsPos)
		{
			if (lhsMin)
			{
				negate(rhs);
			}
			else
			{
				negate();
			}
		}

		if (!rhsPos)
		{
			if (lhsMin)
			{
				rhs = negate(rhs, rhs);
			}
			else
			{
				rhs = negate(rhs, this);
			}
		}

		long[] result = null;
		if (data.length == rhs.data.length)
		{
			result = data;
		}
		else
		{
			result = new long[max];
		}

		while (i < min)
		{
			result[i] = data[i] + rhs.data[i] + carry;
			if ((result[i] & 0x8000000000000000L) != 0)
			{
				carry = 1;
				result[i] = (result[i] & 0x7FFFFFFFFFFFFFFFL);
			}
			else
			{
				carry = 0;
			}

			i++;
		}

		long[] theOne = null;
		if (lhsMin)
		{
			theOne = rhs.data;
		}
		else
		{
			theOne = data;
		}
		while (i < max)
		{
			result[i] = theOne[i] + carry;
			if ((result[i] & 0x8000000000000000L) != 0)
			{
				carry = 1;
				result[i] = (result[i] & 0x7FFFFFFFFFFFFFFFL);
			}
			else
			{
				carry = 0;
			}

			i++;
		}

		this.data = result;
		if (carry == 1 && resultPos != null)
		{
			extendWithCarry();
		}
		else if (carry == 1)
		{
			resultPos = false;
			unNegate();
		}
		else
		{
			resultPos = true;
		}

		this.pos = resultPos;
		this.decPos = resDecPos;
		lock.unlock();
	}

	public double doubleValue()
	{
		if (queued)
		{
			resolveQueue();
		}

		long bits;
		if (pos)
		{
			bits = 0;
		}
		else
		{
			bits = 0x8000000000000000L;
		}

		int i = data.length - 1;
		long fraction = 0;
		int impliedPos = 0;
		while (i >= 0)
		{
			long high = Long.highestOneBit(data[i]);
			if (high == 0)
			{
				i--;
				continue;
			}
			else
			{
				impliedPos = 63 * (i + 1) - Long.numberOfLeadingZeros(data[i]);
				high--;
				if (high > 0)
				{
					// bits to get from this digit
					long toGet = (data[i] & high);
					int bitCount = 64 - Long.numberOfLeadingZeros(high);

					if (bitCount >= 52)
					{
						fraction = (toGet >> (bitCount - 52));
						break;
					}
					else
					{
						fraction = (toGet << (52 - bitCount));
						if (i > 0)
						{
							int remainder = 52 - bitCount;
							// get this many top bits from data[i-1] and shift
							// into low order positions of fraction
							long mask = 1;
							mask = (mask << remainder) - 1;
							int shift = 63 - remainder;
							toGet = (data[i - 1] & (mask << shift));
							fraction += (toGet >> shift);
						}

						break;
					}
				}
				else
				{
					if (i > 0)
					{
						fraction = ((data[i - 1] & 0x7FFFFFFFFFFFF800L) >> 11);
					}
					break;
				}
			}
		}

		bits += fraction;
		// figure out the exponent
		int rShiftPos = impliedPos - decPos;
		rShiftPos += 1023;
		bits += ((rShiftPos & 0x7FFL) << 52);
		return Double.longBitsToDouble(bits);
	}

	private void delayedAdd(BigDecimalReplacement val)
	{
		if (!queued)
		{
			synchronized (this)
			{
				if (!queued)
				{
					queue = new BigDecimalReplacement(0.0);
					queued = true;
				}
			}
		}

		queue.add(val);
		return;
	}

	private void extendWithCarry()
	{
		long[] newData = new long[data.length + 1];
		System.arraycopy(data, 0, newData, 0, data.length);
		newData[data.length] = 1;
		this.data = newData;
	}

	private void lowOrderZeroes(int distance)
	{
		int toAdd = distance / 63;
		long[] result = new long[data.length + toAdd];
		System.arraycopy(data, 0, result, toAdd, data.length);
		data = result;
		decPos = (short)(decPos + toAdd * 63);
	}

	private void moveDec(BigDecimalReplacement toMatch)
	{
		int canMove = Long.numberOfLeadingZeros(data[data.length - 1]) - 1;
		int distance = toMatch.decPos - decPos;
		// long[] result = null;
		if (distance <= canMove)
		{
			// result = new long[toMove.data.length];
			int i = data.length - 1;
			long mask = 1;
			mask = (mask << distance) - 1;
			mask = (mask << (63 - distance));
			int shift = 63 - distance;
			while (i >= 0)
			{
				data[i] = (data[i] << distance);
				if (i > 0)
				{
					data[i] += ((data[i - 1] & mask) >> shift);
				}
				data[i] = (data[i] & 0x7FFFFFFFFFFFFFFFL);
				i--;
			}

			decPos = toMatch.decPos;
			return;
		}

		if (distance > 63)
		{
			lowOrderZeroes(distance);
			distance = toMatch.decPos - decPos;
		}

		long[] result = new long[data.length + 1];

		int i = 0;
		long mask = 1;
		mask = (mask << distance) - 1;
		mask = (mask << (63 - distance));
		while (i < data.length)
		{
			// get everything that will shift out
			long temp = (data[i] & mask);
			result[i] += ((data[i] << distance) & 0x7FFFFFFFFFFFFFFFL);
			result[i + 1] = (temp >> (63 - distance));
			i++;
		}

		data = result;
		decPos = toMatch.decPos;
	}

	private void negate()
	{
		int i = 0;
		while (i < data.length - 1)
		{
			data[i] = ((~data[i]) & 0x7FFFFFFFFFFFFFFFL);
			i++;
		}

		data[i] = (~data[i]);
		i = 0;
		while (i < data.length - 1)
		{
			data[i] += 1;
			if ((data[i] & 0x8000000000000000L) == 0)
			{
				break;
			}
			else
			{
				data[i] = (data[i] & 0x7FFFFFFFFFFFFFFFL);
			}

			i++;
		}

		if (i == data.length - 1)
		{
			data[i] += 1;
		}
	}

	private void negate(BigDecimalReplacement toMatch)
	{
		long[] valData = null;
		if (toMatch.data.length != data.length)
		{
			valData = new long[toMatch.data.length];
			System.arraycopy(data, 0, valData, 0, data.length);
		}
		else
		{
			valData = data;
		}

		int i = 0;
		while (i < valData.length - 1)
		{
			valData[i] = ((~valData[i]) & 0x7FFFFFFFFFFFFFFFL);
			i++;
		}

		valData[i] = (~valData[i]);
		i = 0;
		while (i < valData.length - 1)
		{
			valData[i] += 1;
			if ((valData[i] & 0x8000000000000000L) == 0)
			{
				break;
			}
			else
			{
				valData[i] = (valData[i] & 0x7FFFFFFFFFFFFFFFL);
			}

			i++;
		}

		if (i == valData.length - 1)
		{
			valData[i] += 1;
		}

		data = valData;
	}

	private BigDecimalReplacement negate(BigDecimalReplacement val, BigDecimalReplacement toMatch)
	{
		long[] valData = new long[toMatch.data.length];
		System.arraycopy(val.data, 0, valData, 0, val.data.length);

		int i = 0;
		while (i < valData.length - 1)
		{
			valData[i] = ((~valData[i]) & 0x7FFFFFFFFFFFFFFFL);
			i++;
		}

		valData[i] = (~valData[i]);
		i = 0;
		while (i < valData.length - 1)
		{
			valData[i] += 1;
			if ((valData[i] & 0x8000000000000000L) == 0)
			{
				break;
			}
			else
			{
				valData[i] = (valData[i] & 0x7FFFFFFFFFFFFFFFL);
			}

			i++;
		}

		if (i == valData.length - 1)
		{
			valData[i] += 1;
		}

		return new BigDecimalReplacement(valData, true, val.decPos);
	}

	private void overflow()
	{
		int needed = decPos / 63;
		if (decPos % 63 != 0)
		{
			needed++;
		}

		long[] temp = new long[needed];
		System.arraycopy(data, 0, temp, 0, data.length);
		data = temp;
	}

	private synchronized void resolveQueue()
	{
		if (queue.queued)
		{
			queue.resolveQueue();
		}

		BigDecimalReplacement val = queue;
		BigDecimalReplacement rhs = null;
		short resDecPos;
		if (this.decPos < val.decPos)
		{
			moveDec(val);
			rhs = val;
			resDecPos = val.decPos;
		}
		else if (val.decPos < this.decPos)
		{
			// rhs = moveDec(val, this);
			val.moveDec(this);
			rhs = val;
			resDecPos = this.decPos;
		}
		else
		{
			rhs = val;
			resDecPos = this.decPos;
		}

		short max = (short)Math.max(data.length, rhs.data.length);
		short min = (short)Math.min(data.length, rhs.data.length);
		boolean lhsMin = (data.length == min);
		int i = 0;
		short carry = 0;
		Boolean resultPos = null;
		boolean lhsPos = pos;
		boolean rhsPos = rhs.pos;
		if (lhsPos && rhsPos)
		{
			resultPos = true;
		}
		else if (!lhsPos && !rhsPos)
		{
			resultPos = false;
		}

		if (!lhsPos)
		{
			if (lhsMin)
			{
				negate(rhs);
			}
			else
			{
				negate();
			}
		}

		if (!rhsPos)
		{
			if (lhsMin)
			{
				rhs = negate(rhs, rhs);
			}
			else
			{
				rhs = negate(rhs, this);
			}
		}

		// System.out.print("LHS = ");
		// for (long digit : lhs.data)
		// {
		// System.out.print(" " + digit + " ");
		// }

		// System.out.println("");
		// System.out.print("RHS = ");
		// for (long digit : rhs.data)
		// {
		// System.out.print(" " + digit + " ");
		// }

		// System.out.println("");
		long[] result = null;
		if (data.length == rhs.data.length)
		{
			result = data;
		}
		else
		{
			result = new long[max];
		}

		while (i < min)
		{
			result[i] = data[i] + rhs.data[i] + carry;
			if ((result[i] & 0x8000000000000000L) != 0)
			{
				carry = 1;
				result[i] = (result[i] & 0x7FFFFFFFFFFFFFFFL);
			}
			else
			{
				carry = 0;
			}

			i++;
		}

		while (i < max)
		{
			if (lhsMin)
			{
				result[i] = rhs.data[i] + carry;
				if ((result[i] & 0x8000000000000000L) != 0)
				{
					carry = 1;
					result[i] = (result[i] & 0x7FFFFFFFFFFFFFFFL);
				}
				else
				{
					carry = 0;
				}
			}
			else
			{
				result[i] = data[i] + carry;
				if ((result[i] & 0x8000000000000000L) != 0)
				{
					carry = 1;
					result[i] = (result[i] & 0x7FFFFFFFFFFFFFFFL);
				}
				else
				{
					carry = 0;
				}
			}

			i++;
		}

		this.data = result;
		if (carry == 1 && resultPos != null)
		{
			extendWithCarry();
		}
		else if (carry == 1)
		{
			resultPos = false;
			unNegate();
		}
		else
		{
			resultPos = true;
		}

		// System.out.print("Result = ");
		// for (long digit : result)
		// {
		// System.out.print(" " + digit + " ");
		// }

		// System.out.println("");
		// return new BigDecimalReplacement(result, resultPos, resDecPos);
		this.pos = resultPos;
		this.decPos = resDecPos;
		this.queue = null;
		this.queued = false;
	}

	private void underflow()
	{
		int toAdd = ((-decPos) / 63);
		if ((-decPos) % 63 != 0)
		{
			toAdd++;
		}
		long[] temp = new long[data.length + toAdd];
		System.arraycopy(data, 0, temp, toAdd, data.length);
		data = temp;
		decPos += (63 * toAdd);
	}

	private void unNegate()
	{
		int i = 0;
		while (i < data.length)
		{
			data[i] = ((~data[i]) & 0x7FFFFFFFFFFFFFFFL);
			i++;
		}

		i = 0;
		while (i < data.length)
		{
			data[i] += 1;
			if ((data[i] & 0x8000000000000000L) == 0)
			{
				break;
			}
			else
			{
				data[i] = (data[i] & 0x7FFFFFFFFFFFFFFFL);
			}

			i++;
		}
	}
}
