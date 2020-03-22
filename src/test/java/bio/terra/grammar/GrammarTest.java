package bio.terra.grammar;


import bio.terra.common.category.Unit;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class GrammarTest {

    @Test
    public void testValidate() {
        //String sql = "mr. michael";
        String sql = "SELECT * FROM foo";
        CharStream charStream = CharStreams.fromString(sql);
        SQLLexer lexer = new SQLLexer(charStream);
        SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
        SQLParser.Query_statementContext queryStatement = parser.query_statement();
        Visitor visitor = new Visitor();
        visitor.visit(queryStatement);
    }

}
