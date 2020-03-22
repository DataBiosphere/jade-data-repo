package bio.terra.service.grammar;


import bio.terra.common.category.Unit;
import bio.terra.grammar.BQLLexer;
import bio.terra.grammar.BQLParser;
import bio.terra.grammar.Visitor;
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
        BQLLexer lexer = new BQLLexer(charStream);
        BQLParser parser = new BQLParser(new CommonTokenStream(lexer));
        BQLParser.Query_statementContext queryStatement = parser.query_statement();
        Visitor visitor = new Visitor();
        visitor.visit(queryStatement);
    }

}
