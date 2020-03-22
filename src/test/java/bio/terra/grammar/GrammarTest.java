package bio.terra.grammar;


import bio.terra.common.category.Unit;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@Category(Unit.class)
public class GrammarTest {

    public SQLParser.Query_statementContext parseQuery(String sql) {
        CharStream charStream = CharStreams.fromString(sql);
        SQLLexer lexer = new SQLLexer(charStream);
        SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new BailErrorStrategy());
        return parser.query_statement();
    }

    @Test
    public void testValidate() {
        String sql = "SELECT foo.bar.baz FROM foo.bar";
        Visitor visitor = new Visitor();
        String visitResult = visitor.visit(parseQuery(sql));
        assertThat("visitResult matches", visitResult, equalTo(sql));
    }

    @Test
    public void testBad() {
        String sql = "mr. michael";
        String visitResult = new Visitor().visit(parseQuery(sql));
    }

}
