package harness

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

/**
 * Reusable concurrency support for racing-writer cases. It contributes zero catalog cases while exposing two
 * primitives: concurrent function execution and explicit classification of typed commit conflicts.
 *
 * Both primitives are feature neutral and free of table state, so a feature layer reuses them for its own table mode.
 * The replace-table layer uses them to prove that a replacement racing an append either commits or fails with a typed
 * commit conflict. The general standard concurrency cases live in a follow-up scenario.
 */
object ConcurrencySupport {

  /** How long `runConcurrently` waits for every thread before it reports the stragglers as failures. */
  val completionTimeoutMinutes: Long = 3

  /**
   * Runs every function on its own daemon thread, releases them together, and waits up to
   * `completionTimeoutMinutes` for all of them. Returns the throwables the threads raised, plus one for each thread
   * still running at the deadline. A caller that expects conflicts catches them inside its own function, so a
   * non-empty result always means a thread failed outside the operation under test.
   */
  def runConcurrently(functions: Seq[() => Unit]): Seq[Throwable] = {
    val errors = new ConcurrentLinkedQueue[Throwable]()
    val start = new CountDownLatch(1)
    val threads = functions.zipWithIndex.map { case (function, index) =>
      val thread = new Thread(
        () =>
          try {
            start.await()
            function()
          } catch {
            case interrupted: InterruptedException =>
              Thread.currentThread().interrupt()
              errors.add(interrupted)
            case throwable: Throwable =>
              errors.add(throwable)
          },
        s"delta-harness-concurrent-$index")
      thread.setDaemon(true)
      thread
    }
    threads.foreach(_.start())
    start.countDown()

    val deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(completionTimeoutMinutes)
    threads.foreach { thread =>
      val remainingNanos = deadline - System.nanoTime()
      if (remainingNanos > 0) {
        TimeUnit.NANOSECONDS.timedJoin(thread, remainingNanos)
      }
    }

    threads.filter(_.isAlive).foreach { thread =>
      errors.add(
        new AssertionError(
          s"${thread.getName} did not complete within $completionTimeoutMinutes minutes"))
      thread.interrupt()
    }
    errors.toArray(Array.empty[Throwable]).toSeq
  }

  /** A commit conflict the catalog reports through one of its typed commit, validation or transport exceptions. */
  def isTypedCommitConflict(throwable: Throwable): Boolean =
    Exceptions.causeChain(throwable).exists { cause =>
      val className = cause.getClass.getName
      className.contains("CommitFailed") ||
        className.contains("CommitStateUnknown") ||
        className.contains("Validation") ||
        className.contains("BadRequest") ||
        className.contains("WebClientResponse")
    }

}
