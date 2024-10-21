package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.SelectExpression;
import bio.terra.service.snapshotbuilder.query.SqlExpression;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import java.util.List;
import java.util.stream.Collectors;
import org.stringtemplate.v4.ST;

public class FunctionFilterVariable implements FilterVariable {
  private final FieldVariable fieldVariable;
  private final FunctionTemplate functionTemplate;
  private final List<SelectExpression> values;

  public FunctionFilterVariable(
      FunctionTemplate functionTemplate, FieldVariable fieldVariable, SelectExpression... values) {
    this.functionTemplate = functionTemplate;
    this.fieldVariable = fieldVariable;
    this.values = List.of(values);
    if (values.length == 0) {
      throw new IllegalArgumentException("Function filter values must have at least one value");
    }
    if (values.length != 1
        && (functionTemplate == FunctionTemplate.TEXT_EXACT_MATCH
            || functionTemplate == FunctionTemplate.TEXT_FUZZY_MATCH)) {
      throw new IllegalArgumentException(
          "TEXT_EXACT_MATCH/TEXT_FUZZY_MATCH filter can only match one value");
    }
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    return new ST(functionTemplate.renderSQL(context))
        .add(
            "value",
            values.stream()
                .map(literal -> (literal.renderSQL(context)))
                .collect(Collectors.joining(",")))
        .add("fieldVariable", fieldVariable.renderSQL(context))
        .render();
  }

  public enum FunctionTemplate implements SqlExpression {
    TEXT_EXACT_MATCH(
        "CONTAINS_SUBSTR(<fieldVariable>, <value>)", "CHARINDEX(<value>, <fieldVariable>) > 0"),
    // BigQuery fuzzy match pattern is "bqutil.fn.levenshtein(UPPER(<fieldVariable>),
    // UPPER(<value>))<5"
    TEXT_FUZZY_MATCH("dbo.Levenshtein(UPPER(<fieldVariable>), UPPER(<value>), 5)"),
    IN("<fieldVariable> IN (<value>)"),
    NOT_IN("<fieldVariable> NOT IN (<value>)");

    private final String gcpTemplate;
    private final String azureTemplate;

    FunctionTemplate(String template) {
      this(template, template);
    }

    FunctionTemplate(String gcpTemplate, String azureTemplate) {
      this.gcpTemplate = gcpTemplate;
      this.azureTemplate = azureTemplate;
    }

    @Override
    public String renderSQL(SqlRenderContext context) {
      return context.getPlatform().choose(gcpTemplate, azureTemplate);
    }
  }
}
