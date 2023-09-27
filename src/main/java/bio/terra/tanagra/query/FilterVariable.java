package bio.terra.tanagra.query;

import java.util.List;
import org.stringtemplate.v4.ST;

public abstract class FilterVariable implements SQLExpression {

  protected abstract ST getSubstitutionTemplate(SqlPlatform platform);

  public abstract List<FieldVariable> getFieldVariables();

  @Override
  public String renderSQL(SqlPlatform platform) {
    ST template = getSubstitutionTemplate(platform);
    List<FieldVariable> fieldVars = getFieldVariables();
    for (int ctr = 0; ctr < fieldVars.size(); ctr++) {
      template.add("fieldVariable" + (ctr == 0 ? "" : ctr), fieldVars.get(ctr).renderSqlForWhere());
    }
    return template.render();
  }
}
