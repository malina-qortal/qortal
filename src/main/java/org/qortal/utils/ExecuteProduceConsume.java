package org.qortal.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class ExecuteProduceConsume implements Runnable {

	@XmlAccessorType(XmlAccessType.FIELD)
	public static class StatsSnapshot {
		public int activeThreadCount = 0;
		public int greatestActiveThreadCount = 0;
		public int consumerCount = 0;
		public int tasksProduced = 0;
		public int tasksConsumed = 0;
		public int spawnFailures = 0;

		public StatsSnapshot() {
		}
	}

	private final String className;
	private final Logger logger;

	protected ExecutorService executor;

	// These are volatile to prevent thread-local caching of values
	// but all are updated inside synchronized blocks
	// so we don't need AtomicInteger/AtomicBoolean

	private volatile int activeThreadCount = 0;
	private volatile int greatestActiveThreadCount = 0;
	private volatile int consumerCount = 0;
	private volatile int tasksProduced = 0;
	private volatile int tasksConsumed = 0;
	private volatile int spawnFailures = 0;

	/** Whether a new thread has already been spawned and is waiting to start. Used to prevent spawning multiple new threads. */
	private volatile boolean hasThreadPending = false;

	public ExecuteProduceConsume(ExecutorService executor) {
		this.className = this.getClass().getSimpleName();
		this.logger = LogManager.getLogger(this.getClass());

		this.executor = executor;
	}

	public ExecuteProduceConsume() {
		this(Executors.newCachedThreadPool());
	}

	public void start() {
		this.executor.execute(this);
	}

	public void shutdown() {
		this.executor.shutdownNow();
	}

	public boolean shutdown(long timeout) throws InterruptedException {
		this.executor.shutdownNow();
		return this.executor.awaitTermination(timeout, TimeUnit.MILLISECONDS);
	}

	public StatsSnapshot getStatsSnapshot() {
		StatsSnapshot snapshot = new StatsSnapshot();

		synchronized (this) {
			snapshot.activeThreadCount = this.activeThreadCount;
			snapshot.greatestActiveThreadCount = this.greatestActiveThreadCount;
			snapshot.consumerCount = this.consumerCount;
			snapshot.tasksProduced = this.tasksProduced;
			snapshot.tasksConsumed = this.tasksConsumed;
			snapshot.spawnFailures = this.spawnFailures;
		}

		return snapshot;
	}

	protected void onSpawnFailure() {
		/* Allow override in subclasses */
	}

	/**
	 * Returns a Task to be performed, possibly blocking.
	 * 
	 * @param canBlock
	 * @return task to be performed, or null if no task pending.
	 * @throws InterruptedException
	 */
	protected abstract Task produceTask(boolean canBlock) throws InterruptedException;

	@FunctionalInterface
	public interface Task {
		public abstract void perform() throws InterruptedException;
	}

	@Override
	public void run() {
		Thread.currentThread().setName(this.className + "-" + Thread.currentThread().getId());

		boolean wasThreadPending;
		synchronized (this) {
			++this.activeThreadCount;
			if (this.activeThreadCount > this.greatestActiveThreadCount)
				this.greatestActiveThreadCount = this.activeThreadCount;

			this.logger.trace(() -> String.format("[%d] started, hasThreadPending was: %b, activeThreadCount now: %d",
					Thread.currentThread().getId(), this.hasThreadPending, this.activeThreadCount));

			// Defer clearing hasThreadPending to prevent unnecessary threads waiting to produce...
			wasThreadPending = this.hasThreadPending;
		}

		try {
			while (!Thread.currentThread().isInterrupted()) {
				Task task = null;
				String taskType;

				this.logger.trace(() -> String.format("[%d] waiting to produce...", Thread.currentThread().getId()));

				synchronized (this) {
					if (wasThreadPending) {
						// Clear thread-pending flag now that we about to produce.
						this.hasThreadPending = false;
						wasThreadPending = false;
					}

					// If we're the only non-consuming thread - producer can afford to block this round
					boolean canBlock = this.activeThreadCount - this.consumerCount <= 1;

					this.logger.trace(() -> String.format("[%d] producing... [activeThreadCount: %d, consumerCount: %d, canBlock: %b]",
							Thread.currentThread().getId(), this.activeThreadCount, this.consumerCount, canBlock));

					final long beforeProduce = this.logger.isDebugEnabled() ? System.currentTimeMillis() : 0;

					try {
						task = produceTask(canBlock);
					} catch (InterruptedException e) {
						// We're in shutdown situation so exit
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						this.logger.warn(() -> String.format("[%d] exception while trying to produce task", Thread.currentThread().getId()), e);
					}

					if (this.logger.isDebugEnabled()) {
						final long productionPeriod = System.currentTimeMillis() - beforeProduce;
						taskType = task == null ? "no task" : task.getClass().getCanonicalName();

						this.logger.debug(() -> String.format("[%d] produced [%s] in %dms [canBlock: %b]",
								Thread.currentThread().getId(),
								taskType,
								productionPeriod,
								canBlock
						));
					} else {
						taskType = null;
					}
				}

				if (task == null)
					synchronized (this) {
						this.logger.trace(() -> String.format("[%d] no task, activeThreadCount: %d, consumerCount: %d",
								Thread.currentThread().getId(), this.activeThreadCount, this.consumerCount));

						// If we have an excess of non-consuming threads then we can exit
						if (this.activeThreadCount - this.consumerCount > 1) {
							--this.activeThreadCount;

							this.logger.trace(() -> String.format("[%d] ending, activeThreadCount now: %d",
									Thread.currentThread().getId(), this.activeThreadCount));

							return;
						}

						continue;
					}

				// We have a task

				synchronized (this) {
					++this.tasksProduced;
					++this.consumerCount;

					this.logger.trace(() -> String.format("[%d] hasThreadPending: %b, activeThreadCount: %d, consumerCount now: %d",
							Thread.currentThread().getId(), this.hasThreadPending, this.activeThreadCount, this.consumerCount));

					// If we have no thread pending and no excess of threads then we should spawn a fresh thread
					if (!this.hasThreadPending && this.activeThreadCount == this.consumerCount) {
						this.logger.trace(() -> String.format("[%d] spawning another thread", Thread.currentThread().getId()));

						this.hasThreadPending = true;

						try {
							this.executor.execute(this); // Same object, different thread
						} catch (RejectedExecutionException e) {
							++this.spawnFailures;
							this.hasThreadPending = false;

							this.logger.trace(() -> String.format("[%d] failed to spawn another thread", Thread.currentThread().getId()));

							this.onSpawnFailure();
						}
					} else {
						this.logger.trace(() -> String.format("[%d] NOT spawning another thread", Thread.currentThread().getId()));
					}
				}

				this.logger.trace(() -> String.format("[%d] consuming [%s] task...", Thread.currentThread().getId(), taskType));

				final long beforePerform = this.logger.isDebugEnabled() ? System.currentTimeMillis() : 0;

				try {
					task.perform(); // This can block for a while
				} catch (InterruptedException e) {
					// We're in shutdown situation so exit
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					this.logger.warn(() -> String.format("[%d] exception while consuming task", Thread.currentThread().getId()), e);
				}

				if (this.logger.isDebugEnabled()) {
					final long productionPeriod = System.currentTimeMillis() - beforePerform;

					this.logger.debug(() -> String.format("[%d] consumed [%s] task in %dms", Thread.currentThread().getId(), taskType, productionPeriod));
				}

				synchronized (this) {
					++this.tasksConsumed;
					--this.consumerCount;

					this.logger.trace(() -> String.format("[%d] consumerCount now: %d",
							Thread.currentThread().getId(), this.consumerCount));
				}
			}
		} finally {
			Thread.currentThread().setName(this.className);
		}
	}

}
