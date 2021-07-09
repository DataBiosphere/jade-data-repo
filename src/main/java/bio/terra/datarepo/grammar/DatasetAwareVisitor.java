package bio.terra.grammar;

import bio.terra.grammar.exception.MissingDatasetException;
import bio.terra.model.DatasetModel;
import java.util.Map;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;

public abstract class DatasetAwareVisitor extends SQLBaseVisitor<String> {

  private final Map<String, DatasetModel> datasetMap;

  public DatasetAwareVisitor(Map<String, DatasetModel> datasetMap) {
    this.datasetMap = datasetMap;
  }

  public String getNameFromContext(ParserRuleContext ctx) {
    return ctx == null ? null : ctx.getText();
  }

  public DatasetModel getDatasetByName(String datasetName) {
    if (datasetMap.containsKey(datasetName)) {
      return datasetMap.get(datasetName);
    }
    throw new MissingDatasetException("No dataset found: " + datasetName);
  }

  @Override
  public String visitTerminal(TerminalNode node) {
    String text = node.getText();
    // EOF is included as a terminal -- we don't want it to be included in the output
    return text.equals("<EOF>") ? null : text;
  }

  @Override
  protected String aggregateResult(String aggregate, String nextResult) {
    if (aggregate == null) {
      return nextResult;
    }
    if (nextResult == null) {
      return aggregate;
    }
    // separate tokens with spaces unless we're using dot notation
    String formatString = aggregate.endsWith(".") || nextResult.equals(".") ? "%s%s" : "%s %s";
    return String.format(formatString, aggregate, nextResult);
  }
}
