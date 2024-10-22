package com.linkedin.openhouse.spark.sql.catalyst.parser.extensions

import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseSqlExtensionsParser.QuotedIdentifierContext
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.TerminalNodeImpl
import org.antlr.v4.runtime.{BaseErrorListener, CharStream, CharStreams, CodePointCharStream, CommonToken, CommonTokenStream, IntStream, ParserRuleContext, RecognitionException, Recognizer, Token}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSqlExtensionsPostProcessor
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

import java.util.Locale

class OpenhouseSparkSqlExtensionsParser (delegate: ParserInterface) extends ParserInterface {
  private lazy val astBuilder = new OpenhouseSqlExtensionsAstBuilder(delegate)

  override def parsePlan(sqlText: String): LogicalPlan = {
    if (isOpenhouseCommand(sqlText)) {
      parse(sqlText) { parser => astBuilder.visit(parser.singleStatement()) }.asInstanceOf[LogicalPlan]
    } else {
      delegate.parsePlan(sqlText)
    }
  }

  override def parseExpression(sqlText: String): Expression = {
    delegate.parseExpression(sqlText)
  }

  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }

  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }

  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText);
  }

  def parseRawDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  private def isOpenhouseCommand(sqlText: String): Boolean = {
    val normalized = sqlText.toLowerCase(Locale.ROOT)
      // Strip simple SQL comments that terminate a line, e.g. comments starting with `--` .
      .replaceAll("--.*?\\n", " ")
      // Strip newlines.
      .replaceAll("\\s+", " ")
      // Strip comments of the form  /* ... */. This must come after stripping newlines so that
      // comments that span multiple lines are caught.
      .replaceAll("/\\*.*?\\*/", " ")
      // Strip doubles spaces post comments extraction, ex: ALTER /* ... \n ...*/ TABLE
      .replaceAll("\\s+", " ")
      .trim()
    (normalized.startsWith("alter table") &&
      (normalized.contains("set policy")) ||
      (normalized.contains("modify column") &&
        normalized.contains("set tag"))) ||
      normalized.startsWith("grant") ||
      normalized.startsWith("revoke") ||
      normalized.startsWith("show grants")

  }

  protected def parse[T](command: String)(toResult: OpenhouseSqlExtensionsParser => T): T = {
    val lexer = new OpenhouseSqlExtensionsLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(OpenhouseParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new OpenhouseSqlExtensionsParser(tokenStream)
    parser.addParseListener(OpenhouseSqlExtensionsPostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(OpenhouseParseErrorListener)

    toResult(parser)
  }

  override def parseQuery(sqlText: String): LogicalPlan = {
    parsePlan(sqlText)
  }
}

/* Copied from Apache Spark's to avoid dependency on Spark Internals */
class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume
  override def getSourceName(): String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = wrapped.getText(interval)

  // scalastyle:off
  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
  // scalastyle:on
}

case object OpenhouseParseErrorListener extends BaseErrorListener {
  override def syntaxError(
    recognizer: Recognizer[_, _],
    offendingSymbol: scala.Any,
    line: Int,
    charPositionInLine: Int,
    msg: String,
    e: RecognitionException): Unit = {
      throw new OpenhouseParseException(msg, line, charPositionInLine)
  }
}

/* Extends AnalysisException to access protected constructor */
class OpenhouseParseException(
  message: String,
  line: Int,
  startPosition: Int) extends AnalysisException(message, Some(line), Some(startPosition)) {}

case object OpenhouseSqlExtensionsPostProcessor extends OpenhouseSqlExtensionsBaseListener {
  override def exitQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {
    val token = ctx.BACKQUOTED_IDENTIFIER.getSymbol().asInstanceOf[CommonToken]
    token.setText(token.getText.replace("`", ""))
  }
}