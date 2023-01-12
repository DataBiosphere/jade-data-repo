package bio.terra.grammar;

import bio.terra.grammar.exception.InvalidQueryException;
import java.util.List;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

public class Query {

  private final SQLParser parser;
  private final SQLParser.Query_statementContext queryStatement;

  private Query(SQLParser parser, SQLParser.Query_statementContext queryStatement) {
    this.parser = parser;
    this.queryStatement = queryStatement;
  }

  public static Query parse(String sql) {
    CharStream charStream = CharStreams.fromString(sql);
    SQLLexer lexer = new SQLLexer(charStream);
    SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
    parser.setErrorHandler(new BailErrorStrategy());
    try {
      return new Query(parser, parser.query_statement());
    } catch (ParseCancellationException ex) {
      throw new InvalidQueryException("Could not parse query: " + sql, ex);
    }
  }

  public List<String> getDatasetNames() {
    DatasetNameListener listener = new DatasetNameListener();
    ParseTreeWalker.DEFAULT.walk(listener, queryStatement);
    return listener.getDatasetNames();
  }
  // We currently make the assumption that there is only one dataset
  // (based on the validation flight step that already occurred.)
  // This will change when more than 1 dataset is allowed
  public String getDatasetName() {
    return getDatasetNames().get(0);
  }

  public List<String> getTableNames() {
    TableNameListener listener = new TableNameListener();
    ParseTreeWalker.DEFAULT.walk(listener, queryStatement);
    return listener.getTableNames();
  }

  public List<String> getColumnNames() {
    ColumnNameListener listener = new ColumnNameListener();
    ParseTreeWalker.DEFAULT.walk(listener, queryStatement);
    return listener.getColumnNames();
  }

  public String translateSql(SQLBaseVisitor<String> visitor) {
    return visitor.visit(queryStatement);
  }
}
