package bio.terra.grammar;

import org.antlr.v4.runtime.ParserRuleContext;

public class Visitor extends BQLBaseVisitor<String> {

    String getNameFromContext(ParserRuleContext ctx) {
        return ctx == null ? null : ctx.getText();
    }

    @Override
    public String visitTable_expr(BQLParser.Table_exprContext ctx) {
        String datasetName = getNameFromContext(ctx.dataset_name());
        String tableName = getNameFromContext(ctx.table_name());
        System.out.println(String.format("%s.%s", datasetName, tableName));
        return visitChildren(ctx);
    }
}
