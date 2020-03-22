package bio.terra.grammar;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

public class Visitor extends SQLBaseVisitor<String> {

    private String getNameFromContext(ParserRuleContext ctx) {
        return ctx == null ? null : ctx.getText();
    }

    @Override
    public String visitTable_expr(SQLParser.Table_exprContext ctx) {
        String datasetName = getNameFromContext(ctx.dataset_name());
        String tableName = getNameFromContext(ctx.table_name());
        return String.format("%s.%s", datasetName, tableName);
    }

    @Override
    public String visitColumn_expr(SQLParser.Column_exprContext ctx) {
        String datasetName = getNameFromContext(ctx.dataset_name());
        String tableName = getNameFromContext(ctx.table_name());
        String columnName = getNameFromContext(ctx.column_name());
        return String.format("%s.%s.%s", datasetName, tableName, columnName);
    }

    @Override
    public String visitTerminal(TerminalNode node) {
        return node.getText();
    }

    @Override
    public String visitChildren(RuleNode node) {
        String result = defaultResult();
        int n = node.getChildCount();
        for (int i = 0; i < n; i++) {
            if (!shouldVisitNextChild(node, result)) {
                break;
            }

            ParseTree c = node.getChild(i);
            String childResult = c.accept(this);
            result = aggregateResult(result, childResult);
        }

        return result;
    }

    protected String aggregateResult(String aggregate, String nextResult) {
        if (aggregate == null) {
            return nextResult;
        }
        if (nextResult == null) {
            return aggregate;
        }
        return String.format("%s %s", aggregate, nextResult);
    }
}
