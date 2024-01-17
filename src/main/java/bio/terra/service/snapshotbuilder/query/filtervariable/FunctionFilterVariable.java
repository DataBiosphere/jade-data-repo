package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.SqlExpression;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.List;
import java.util.stream.Collectors;
import org.stringtemplate.v4.ST;

public class FunctionFilterVariable implements FilterVariable {
  private final FieldVariable fieldVariable;
  private final FunctionTemplate functionTemplate;
  private final List<Literal> values;

  public FunctionFilterVariable(
      FunctionTemplate functionTemplate, FieldVariable fieldVariable, Literal... values) {
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
  public String renderSQL() {
    return new ST(functionTemplate.renderSQL())
        .add("value", values.stream().map(Literal::renderSQL).collect(Collectors.joining(",")))
        .add("fieldVariable", fieldVariable.renderSqlForWhere())
        .render();
  }

  @Override
  public List<TableVariable> getTables() {
    return List.of(fieldVariable.getTableVariable());
  }

  public enum FunctionTemplate implements SqlExpression {
    TEXT_EXACT_MATCH("CONTAINS_SUBSTR(<fieldVariable>, <value>)"),
    // BigQuery fuzzy match pattern is "bqutil.fn.levenshtein(UPPER(<fieldVariable>),
    // UPPER(<value>))<5"
    TEXT_FUZZY_MATCH("dbo.Levenshtein(UPPER(<fieldVariable>), UPPER(<value>), 5)"),
    IN("<fieldVariable> IN (<value>)"),
    NOT_IN("<fieldVariable> NOT IN (<value>)");

    private final String template;

    FunctionTemplate(String template) {
      this.template = template;
    }

    @Override
    public String renderSQL() {
      return template;
    }
  }
}
