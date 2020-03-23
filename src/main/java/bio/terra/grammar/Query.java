package bio.terra.grammar;

import bio.terra.grammar.exception.InvalidQueryException;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.List;


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

    public String translateSql(SQLBaseVisitor<String> visitor) {
        return visitor.visit(queryStatement);
    }

}
