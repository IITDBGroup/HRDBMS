package com.exascale.misc;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

/**
 * <h1>Scalable Stamped Read-Write Lock</h1> A Read-Write Lock that is scalable
 * with the number of threads doing Read.
 * <p>
 * Uses a two-state-machine for the Readers, and averages two synchronized
 * operations. <br>
 * Although this mechanism was independently designed and implemented by the
 * authors, the idea is very similar to the algorithm C-RW-WP described in this
 * paper: <a href=
 * "http://blogs.oracle.com/dave/resource/ppopp13-dice-NUMAAwareRWLocks.pdf">
 * NUMA-Aware Reader-Writer locks</a> <br>
 * Relative to the paper, there are two differences: The threads have no
 * particular order, which means this implementation is <b>not</b> NUMA-aware;
 * Threads attempting a read-lock for the first time are added to a list and
 * removed when the thread terminates, following the mechanism described below.
 * <p>
 * To manage the adding and removal of new Reader threads, we use a
 * ConcurrentLinkedQueue instance named {@code readersStateList} containing all
 * the references to ReadersEntry (Reader's states), which the Writer scans to
 * determine if the Readers have completed or not. After a thread terminates,
 * the {@code finalize()} of the associated {@code ReaderEntry} instance will be
 * called, which will remove the Reader's state reference from the
 * {@code readersStateList}, to avoid memory leaking.
 * <p>
 * Relatively to the ScalableRWLock implemented previously, we use a StampedLock
 * instead of a regular lock so that we can default to the StampedLock's
 * operations of readLock() and writeLock() whenever the Writer contention is
 * high. This kind of technique is partially described in the paper mentioned
 * above.
 * <p>
 * Advantages:
 * <ul>
 * <li>Implements {@code java.util.concurrent.locks.ReadWriteLock}
 * <li>When there are very few Writes, the performance scales with the number of
 * Reader threads
 * <li>Supports optimistic Reads like the StampedLock
 * <li>Fairness proprieties are similar to StampedLock
 * <li>No need to call initialization/cleanup functions per thread
 * <li>No limitation on the number of concurrent threads
 * </ul>
 * <p>
 * Disadvantages:
 * <ul>
 * <li>Not Reentrant
 * <li>Memory footprint increases with number of threads by sizeof(ReadersEntry)
 * x O(N_threads)
 * <li>Does not support {@code lockInterruptibly()}
 * <li>Does not support {@code newCondition()}
 * </ul>
 * <p>
 * For scenarios with few writes, the average case for {@code sharedLock()} is
 * two synchronized calls: an {@code AtomicInteger.set()} on a cache line that
 * is held in exclusive mode by the core where the current thread is running,
 * and an {@code AtomicLong.get()} on a shared cache line.<br>
 * This means that when doing several sequential calls of sharedLock()/unlock()
 * on the same instance, the performance penalty will be small because the
 * accessed variables will most likely be in L1/L2 cache.
 *
 * @author Pedro Ramalhete
 * @author Andreia Correia
 */
public class ScalableStampedRWLock implements ReadWriteLock, java.io.Serializable
{

	private static final long serialVersionUID = -1275456836855114993L;

	// Reader states
	private final static int SRWL_STATE_NOT_READING = 0;
	private final static int SRWL_STATE_READING = 1;

	/**
	 * We use this as a "special" marking: readersStateArrayRef will point to it
	 * when the array of Reader's states needs to be rebuild from the
	 * ConcurrentLinkedQueue.
	 */
	private transient final static AtomicInteger[] dummyArray = new AtomicInteger[0];

	/**
	 * List of Reader's states that the Writer will scan when attempting to
	 * acquire the lock in write-mode
	 */
	private transient final ConcurrentLinkedQueue<AtomicInteger> readersStateList;

	/**
	 * Stamped lock that is used mostly as writer-lock
	 */
	private transient final StampedLock stampedLock;

	/**
	 * Thread-local reference to the current thread's ReadersEntry instance.
	 * It's from this instance that the current Reader thread is able to
	 * determine where to store its own state for that particular thread.
	 */
	private transient final ThreadLocal<ReadersEntry> entry;

	/**
	 * Shortcut to the reader's states so that we don't have to walk the
	 * ConcurrentLinkedQueue on every exclusiveLock().
	 */
	private transient final AtomicReference<AtomicInteger[]> readersStateArrayRef;

	/**
	 * The lock returned by method {@link ScalableReentrantRWLock#readLock}.
	 */
	private final InnerReadLock readerLock;

	/**
	 * The lock returned by method {@link ScalableReentrantRWLock#writeLock}.
	 */
	private final InnerWriteLock writerLock;

	/**
	 * Default constructor
	 */
	public ScalableStampedRWLock()
	{
		// States of the Readers, one entry in the list per thread
		readersStateList = new ConcurrentLinkedQueue<AtomicInteger>();
		stampedLock = new StampedLock();
		entry = new ThreadLocal<ReadersEntry>();
		readersStateArrayRef = new AtomicReference<AtomicInteger[]>(null);
		readerLock = new ScalableStampedRWLock.InnerReadLock(this);
		writerLock = new ScalableStampedRWLock.InnerWriteLock(this);
	}

	/**
	 * Acquires the write lock.
	 *
	 * <p>
	 * Acquires the write lock if neither the read nor write lock are held by
	 * another thread and returns immediately.
	 *
	 * <p>
	 * If the lock is held by another thread, then the current thread yields and
	 * lies dormant until the write lock has been acquired.
	 */
	public void exclusiveLock()
	{
		// Try to acquire the stampedLock in write-mode
		stampedLock.writeLock();

		// We can only do this after the stampedLock has been acquired
		AtomicInteger[] localReadersStateArray = readersStateArrayRef.get();
		if (localReadersStateArray == null)
		{
			// Set to dummyArray before scanning the readersStateList to impose
			// a linearizability condition
			readersStateArrayRef.set(dummyArray);
			// Copy readersStateList to an array
			localReadersStateArray = readersStateList.toArray(new AtomicInteger[readersStateList.size()]);
			readersStateArrayRef.compareAndSet(dummyArray, localReadersStateArray);
		}

		// Scan the array of Reader states
		for (AtomicInteger readerState : localReadersStateArray)
		{
			while (readerState != null && readerState.get() == SRWL_STATE_READING)
			{
				Thread.yield();
			}
		}
	}

	/**
	 * Acquires the write lock only if it is not held by another thread at the
	 * time of invocation.
	 *
	 * <p>
	 * Acquires the write lock if the write lock is not held by another thread
	 * and returns immediately with the value {@code true} if and only if no
	 * other thread is attempting a read lock.
	 *
	 * <p>
	 * If the write lock is held by another thread then this method will return
	 * immediately with the value {@code false}.
	 *
	 * @return {@code true} if the write lock was free and was acquired by the
	 *         current thread and {@code false} otherwise.
	 */
	public boolean exclusiveTryLock()
	{
		// Try to acquire the stampedLock in write-mode
		if (stampedLock.tryWriteLock() == 0)
		{
			return false;
		}

		// We can only do this after the stampedLock has been acquired
		AtomicInteger[] localReadersStateArray = readersStateArrayRef.get();
		if (localReadersStateArray == null)
		{
			// Set to dummyArray before scanning the readersStateList to impose
			// a linearizability condition
			readersStateArrayRef.set(dummyArray);
			// Copy readersStateList to an array
			localReadersStateArray = readersStateList.toArray(new AtomicInteger[readersStateList.size()]);
			readersStateArrayRef.compareAndSet(dummyArray, localReadersStateArray);
		}

		// Scan the array of Reader states
		for (AtomicInteger readerState : localReadersStateArray)
		{
			if (readerState != null && readerState.get() == SRWL_STATE_READING)
			{
				stampedLock.asWriteLock().unlock();
				return false;
			}
		}

		return true;
	}

	/**
	 * Acquires the write lock if it is not held by another thread within the
	 * given waiting time.
	 *
	 * <p>
	 * Acquires the write lock if the write lock is not held by another thread
	 * and returns immediately with the value {@code true} if and only if no
	 * other thread is attempting a read lock, setting the write lock
	 * {@code reentrantWriterCount} to one. If another thread is attempting a
	 * read lock, this function <b>may yield until the read lock is
	 * released</b>.
	 *
	 * <p>
	 * If the write lock is held by another thread then the current thread
	 * yields and lies dormant until one of two things happens:
	 * <ul>
	 * <li>The write lock is acquired by the current thread; or
	 * <li>The specified waiting time elapses
	 * </ul>
	 *
	 * <p>
	 * If the read lock is held by another thread then the current thread yields
	 * and lies dormant until the read lock is released.
	 *
	 * <p>
	 * If the write lock is acquired then the value {@code true} is returned and
	 * the write lock {@code reentrantWriterCount} is set to one.
	 *
	 * <p>
	 * There is no guarantee that there is a maximum waiting time for this
	 * method.
	 *
	 * @param nanosTimeout
	 *            the time to wait for the write lock in nanoseconds
	 *
	 * @return {@code true} if the lock was free and was acquired by the current
	 *         thread, or the write lock was already held by the current thread;
	 *         and {@code false} if the waiting time elapsed before the lock
	 *         could be acquired.
	 */
	public boolean exclusiveTryLockNanos(long nanosTimeout) throws InterruptedException
	{
		final long lastTime = System.nanoTime();
		// Try to acquire the stampedLock in write-mode
		if (stampedLock.tryWriteLock(nanosTimeout, TimeUnit.NANOSECONDS) == 0)
		{
			return false;
		}

		// We can only do this after the stampedLock has been acquired
		AtomicInteger[] localReadersStateArray = readersStateArrayRef.get();
		if (localReadersStateArray == null)
		{
			// Set to dummyArray before scanning the readersStateList to impose
			// a linearizability condition
			readersStateArrayRef.set(dummyArray);
			// Copy readersStateList to an array
			localReadersStateArray = readersStateList.toArray(new AtomicInteger[readersStateList.size()]);
			readersStateArrayRef.compareAndSet(dummyArray, localReadersStateArray);
		}

		// Scan the array of Reader states
		for (AtomicInteger readerState : localReadersStateArray)
		{
			while (readerState != null && readerState.get() == SRWL_STATE_READING)
			{
				if (System.nanoTime() - lastTime < nanosTimeout)
				{
					Thread.yield();
				}
				else
				{
					// Time has expired and there is still at least one Reader
					// so give up
					stampedLock.asWriteLock().unlock();
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * Attempts to release the write lock.
	 *
	 * If the current thread is not the holder of this lock then
	 * {@link IllegalMonitorStateException} is thrown.
	 *
	 * @throws IllegalMonitorStateException
	 *             if the current thread does not hold this lock.
	 */
	public void exclusiveUnlock()
	{
		if (!stampedLock.isWriteLocked())
		{
			// ERROR: tried to unlock a non write-locked instance
			throw new IllegalMonitorStateException();
		}
		stampedLock.asWriteLock().unlock();
	}

	@Override
	public Lock readLock()
	{
		return readerLock;
	}

	/**
	 * Acquires the read lock.
	 *
	 * <p>
	 * Acquires the read lock if the write lock is not held by another thread
	 * and returns immediately.
	 *
	 * <p>
	 * If the write lock is held by another thread then the current thread
	 * yields until the write lock is released.
	 */
	public void sharedLock()
	{
		ReadersEntry localEntry = entry.get();
		// Initialize a new Reader-state for this thread if needed
		if (localEntry == null)
		{
			localEntry = addState();
		}

		final AtomicInteger currentReadersState = localEntry.state;
		// The "optimistic" code path uses only two synchronized calls:
		// a set() on a cache line that should be held in exclusive mode
		// by the current thread, and a get() on a cache line that is shared.
		while (true)
		{
			currentReadersState.set(SRWL_STATE_READING);
			if (!stampedLock.isWriteLocked())
			{
				// Acquired lock in read-only mode
				return;
			}
			else
			{
				// Go back to SRWL_STATE_NOT_READING to avoid blocking a Writer
				currentReadersState.set(SRWL_STATE_NOT_READING);
				// If there is a Writer, we go for the StampedLock.readlock()
				// instead of the ScalableRWLock method
				if (stampedLock.isWriteLocked())
				{
					stampedLock.asReadLock().lock();
					localEntry.isStampLocked = true;
					return;
				}
			}
		}
	}

	/**
	 * Acquires the read lock only if the write lock is not held by another
	 * thread at the time of invocation.
	 *
	 * <p>
	 * Acquires the read lock if the write lock is not held by another thread
	 * and returns immediately with the value {@code true}.
	 *
	 * <p>
	 * If the write lock is held by another thread then this method will return
	 * immediately with the value {@code false}.
	 *
	 * @return {@code true} if the read lock was acquired
	 */
	public boolean sharedTryLock()
	{
		ReadersEntry localEntry = entry.get();
		// Initialize a new Reader-state for this thread if needed
		if (localEntry == null)
		{
			localEntry = addState();
		}

		final AtomicInteger currentReadersState = localEntry.state;
		// The "optimistic" code path takes only two synchronized calls:
		// a set() on a cache line that should be held in exclusive mode
		// by the current thread, and a get() on a cache line that is shared.
		currentReadersState.set(SRWL_STATE_READING);
		if (!stampedLock.isWriteLocked())
		{
			// Acquired lock in read-only mode
			return true;
		}
		else
		{
			currentReadersState.set(SRWL_STATE_NOT_READING);
			return false;
		}
	}

	/**
	 * Acquires the read lock if the write lock is not held by another thread
	 * within the given waiting time.
	 *
	 * <p>
	 * Acquires the read lock if the write lock is not held by another thread
	 * and returns immediately with the value {@code true}.
	 *
	 * <p>
	 * If the write lock is held by another thread then the current thread
	 * yields execution until one of two things happens:
	 * <ul>
	 * <li>The read lock is acquired by the current thread; or
	 * <li>The specified waiting time elapses.
	 * </ul>
	 *
	 * <p>
	 * If the read lock is acquired then the value {@code true} is returned.
	 *
	 * @param nanosTimeout
	 *            the time to wait for the read lock in nanoseconds
	 * @return {@code true} if the read lock was acquired
	 */
	public boolean sharedTryLockNanos(long nanosTimeout)
	{
		final long lastTime = System.nanoTime();
		ReadersEntry localEntry = entry.get();
		// Initialize a new Reader-state for this thread if needed
		if (localEntry == null)
		{
			localEntry = addState();
		}

		final AtomicInteger currentReadersState = localEntry.state;
		// The "optimistic" code path takes only two synchronized calls:
		// a set() on a cache line that should be held in exclusive mode
		// by the current thread, and a get() on a cache line that is shared.
		while (true)
		{
			currentReadersState.set(SRWL_STATE_READING);
			if (!stampedLock.isWriteLocked())
			{
				// Acquired lock in read-only mode
				return true;
			}
			else
			{
				// Go back to SRWL_STATE_NOT_READING to avoid blocking a Writer
				currentReadersState.set(SRWL_STATE_NOT_READING);
				if (nanosTimeout <= 0)
				{
					return false;
				}
				while (stampedLock.isWriteLocked())
				{
					// Some (other) thread is holding the write-lock, we must
					// wait
					if (System.nanoTime() - lastTime < nanosTimeout)
					{
						Thread.yield();
					}
					else
					{
						return false;
					}
				}
			}
		}
	}

	/**
	 * Attempts to release the read lock.
	 *
	 * If the current thread is not the holder of this lock then
	 * {@link IllegalMonitorStateException} is thrown.
	 *
	 * @throws IllegalMonitorStateException
	 *             if the current thread does not hold this lock.
	 */
	public void sharedUnlock()
	{
		final ReadersEntry localEntry = entry.get();
		if (localEntry == null)
		{
			// ERROR: Tried to unlock a non read-locked lock
			throw new IllegalMonitorStateException();
		}
		else
		{
			if (localEntry.isStampLocked)
			{
				localEntry.isStampLocked = false;
				stampedLock.asReadLock().unlock();
			}
			else
			{
				localEntry.state.set(SRWL_STATE_NOT_READING);
			}
			return;
		}
	}

	/**
	 * Returns a stamp that can later be validated, or zero if exclusively
	 * locked.
	 *
	 * @return a stamp, or zero if exclusively locked
	 */
	public long tryOptimisticRead()
	{
		return stampedLock.tryOptimisticRead();
	}

	/**
	 * Returns true if the lock has not been exclusively acquired since issuance
	 * of the given stamp. Always returns false if the stamp is zero. Always
	 * returns true if the stamp represents a currently held lock. Invoking this
	 * method with a value not obtained from {@link #tryOptimisticRead} has no
	 * defined effect or result.
	 *
	 * @return true if the lock has not been exclusively acquired since issuance
	 *         of the given stamp; else false
	 */
	public boolean validate(long stamp)
	{
		return stampedLock.validate(stamp);
	}

	@Override
	public Lock writeLock()
	{
		return writerLock;
	}

	/**
	 * Creates a new ReadersEntry instance for the current thread and its
	 * associated AtomicInteger to store the state of the Reader
	 *
	 * @return Returns a reference to the newly created instance of
	 *         {@code ReadersEntry}
	 */
	private ReadersEntry addState()
	{
		final AtomicInteger state = new AtomicInteger(SRWL_STATE_NOT_READING);
		final ReadersEntry newEntry = new ReadersEntry(state);
		entry.set(newEntry);
		readersStateList.add(state);
		// Setting the readersStateArrayRef to null will make the Writer
		// rebuild the array from the CLQ.
		readersStateArrayRef.set(null);
		return newEntry;
	}

	/**
	 * This function should be called only from ReadersEntry.finalize()
	 *
	 * @param state
	 *            The reader's state that we wish to remove from the
	 *            ConcurrentLinkedQueue
	 */
	protected void removeState(AtomicInteger state)
	{
		readersStateList.remove(state);
		// Setting the readersStateArrayRef to null will make the Writer
		// rebuild the array from the CLQ.
		readersStateArrayRef.set(null);

		// Paranoia: just in case someone forgot to call sharedUnlock()
		// and there is a Writer waiting on that state.
		state.set(SRWL_STATE_NOT_READING);
	}

	/**
	 * Read-only lock
	 */
	final class InnerReadLock implements Lock
	{
		final ScalableStampedRWLock rwlock;

		public InnerReadLock(ScalableStampedRWLock rwlock)
		{
			this.rwlock = rwlock;
		}

		@Override
		public void lock()
		{
			rwlock.sharedLock();
		}

		@Override
		public void lockInterruptibly() throws InterruptedException
		{
			// Not supported
			throw new UnsupportedOperationException();
		}

		@Override
		public Condition newCondition()
		{
			// Not supported
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean tryLock()
		{
			return rwlock.sharedTryLock();
		}

		@Override
		public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException
		{
			if (Thread.interrupted())
			{
				throw new InterruptedException();
			}
			return sharedTryLockNanos(unit.toNanos(timeout));
		}

		@Override
		public void unlock()
		{
			rwlock.sharedUnlock();
		}
	}

	/**
	 * Write-only lock
	 */
	final class InnerWriteLock implements Lock
	{
		final ScalableStampedRWLock rwlock;

		public InnerWriteLock(ScalableStampedRWLock rwlock)
		{
			this.rwlock = rwlock;
		}

		@Override
		public void lock()
		{
			rwlock.exclusiveLock();
		}

		@Override
		public void lockInterruptibly() throws InterruptedException
		{
			// Not supported
			throw new UnsupportedOperationException();
		}

		@Override
		public Condition newCondition()
		{
			// Not supported
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean tryLock()
		{
			return rwlock.exclusiveTryLock();
		}

		@Override
		public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException
		{
			if (Thread.interrupted())
			{
				throw new InterruptedException();
			}
			return exclusiveTryLockNanos(unit.toNanos(timeout));
		}

		@Override
		public void unlock()
		{
			rwlock.exclusiveUnlock();
		}
	}

	/**
	 * Inner class that makes use of finalize() to remove the Reader's state
	 * from the ConcurrentLinkedQueue {@code readersStateList}
	 */
	final class ReadersEntry
	{
		public final AtomicInteger state;
		public boolean isStampLocked = false;

		public ReadersEntry(AtomicInteger state)
		{
			this.state = state;
		}

		@Override
		protected void finalize() throws Throwable
		{
			removeState(state);
			super.finalize();
		}
	}
}
