package harness

import scala.annotation.tailrec
import scala.util.Try
import scala.util.control.NonFatal

final class HarnessConfigurationException(message: String) extends Exception(message)

private[harness] object RunnerConfiguration {
  def parallelism(environment: Map[String, String], availableProcessors: Int): Int =
    environment.get("HARNESS_PARALLELISM") match {
      case None => math.max(1, availableProcessors)
      case Some(value) =>
        Try(value.toInt).toOption.filter(_ > 0).getOrElse(
          throw new HarnessConfigurationException(
            s"HARNESS_PARALLELISM must be a positive integer; received '$value'"))
    }
}

/** Executes one case under the harness retry policy. */
object Runner {
  val MaxAttempts = 3

  /** Runs a case, retrying only a transient infrastructure failure. */
  def execute(testCase: TestCase, context: Ctx): (Outcome, Int) = {
    @tailrec def attempt(attemptIndex: Int): (Outcome, Int) = {
      val outcome =
        try {
          testCase.run(context.copy(spark = context.spark.newSession()))
          Outcome.Passed
        } catch {
          case NonFatal(throwable) => Outcome.Failed(throwable)
        }

      outcome match {
        case failure: Outcome.Failed
            if failure.retryable && attemptIndex + 1 < MaxAttempts =>
          attempt(attemptIndex + 1)
        case terminal =>
          (terminal, attemptIndex + 1)
      }
    }

    attempt(0)
  }
}
