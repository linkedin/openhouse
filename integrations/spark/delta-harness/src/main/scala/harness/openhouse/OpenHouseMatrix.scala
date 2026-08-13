package harness

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.iceberg.exceptions.BadRequestException
import org.apache.iceberg.exceptions.ValidationException
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

/**
 * The concrete tests, all on CoreTable. An operation is a HEADLESS pipeline segment (no create);
 * the run crosses every operation with every `Layout` by composing `createAndSeed(layout)` before
 * it via `andThen`. Every operation asserts the DELTA against the observed pre-state (rows and/or
 * commit count), never an absolute row set — so a test holds under any layout. Operation sources
 * are written as EXPLICIT literals.
 */
// The tests are authored across cohesive per-domain traits (see *Scenarios.scala + ScenarioKit.scala);
// this object assembles them. Trait mixin order == original top-to-bottom source order, so val
// initialization order is preserved. `object Plan` consumes the public members declared here.
object Scenarios extends MorMaintScenarios with DmlScenarios with NestedTypesScenarios with MaintControlScenarios with ForkScenarios with BranchWapScenarios with NegativeDdlScenarios with InteractionScenarios with SurfaceScenarios with HazardReaderWriterScenarios
