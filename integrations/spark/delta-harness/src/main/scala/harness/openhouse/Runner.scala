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

/** Executes one case, retrying only transient session creation failures before the case starts. */
object Runner {
  val MaxAttempts = 3

  /** Runs a case. Once the case body starts, every failure is terminal because observable state may have changed. */
  def execute(testCase: TestCase, context: Ctx): (Outcome, Int) = {
    @tailrec def attempt(attemptIndex: Int): (Outcome, Int) = {
      val attemptContext =
        try {
          Right(context.copy(spark = context.spark.newSession()))
        } catch {
          case NonFatal(throwable) => Left(Outcome.Failed(throwable))
        }

      attemptContext match {
        case Left(failure)
            if Exceptions.isTransientConnectionFailure(failure.cause) &&
              attemptIndex + 1 < MaxAttempts =>
          attempt(attemptIndex + 1)
        case Left(failure) =>
          (failure, attemptIndex + 1)
        case Right(caseContext) =>
          val outcome =
            try {
              testCase.run(caseContext)
              Outcome.Passed
            } catch {
              case NonFatal(throwable) => Outcome.Failed(throwable)
            }
          (outcome, attemptIndex + 1)
      }
    }

    attempt(0)
  }
}
