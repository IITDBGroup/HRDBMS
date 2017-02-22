package com.exascale.misc;

public class RunningBitmapMerge implements BitmapSymmetricAlgorithm
{

	@Override
	public void symmetric(final UpdateableBitmapFunction f, final BitmapStorage out, final CompressedBitSet... set)
	{
		out.clear();
		final PriorityQ<EWAHPointer> h = new PriorityQ<EWAHPointer>(set.length, (arg0, arg1) -> arg0.compareTo(arg1));
		f.resize(set.length);

		for (int k = 0; k < set.length; ++k)
		{
			final EWAHPointer x = new EWAHPointer(0, new IteratingBufferedRunningLengthWord(set[k]), k);
			if (x.hasNoData())
			{
				continue;
			}
			f.rw[k] = x;
			x.callbackUpdate(f);
			h.toss(x);
		}
		h.buildHeap(); // just in case we use an insane number of inputs

		int lasta = 0;
		if (h.isEmpty())
		{
			return;
		}
		mainloop: while (true)
		{ // goes until no more active inputs
			final int a = h.peek().endOfRun();
			// I suppose we have a run of length a - lasta here.
			f.dispatch(out, lasta, a);
			lasta = a;

			while (h.peek().endOfRun() == a)
			{
				final EWAHPointer p = h.peek();
				p.parseNextRun();
				p.callbackUpdate(f);
				if (p.hasNoData())
				{
					h.poll(); // we just remove it
					if (h.isEmpty())
					{
						break mainloop;
					}
				}
				else
				{
					h.percolateDown(); // since we have
					// increased the key
				}
			}
		}
	}

}