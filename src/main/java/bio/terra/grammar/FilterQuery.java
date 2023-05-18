package bio.terra.grammar;

import bio.terra.grammar.exception.InvalidQueryException;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;

public class FilterQuery {

  private final SQLParser parser;
  private final SQLParser.Where_statementContext filterStatement;

  private FilterQuery(SQLParser parser, SQLParser.Where_statementContext filterStatement) {
    this.parser = parser;
    this.filterStatement = filterStatement;
  }

  public static FilterQuery parseWhereClause(String filter) {
    CharStream charStream = CharStreams.fromString(filter);
    SQLLexer lexer = new SQLLexer(charStream);
    SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
    parser.setErrorHandler(new BailErrorStrategy());
    try {
      return new FilterQuery(parser, parser.where_statement());
    } catch (ParseCancellationException ex) {
      throw new InvalidQueryException("Could not parse filter Statement: " + filter, ex);
    }
  }
}
