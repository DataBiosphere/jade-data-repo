package bio.terra.tanagra.underlay;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.serialization.displayhint.UFValueDisplay;

public class ValueDisplay {
  private final Literal value;
  private final String display;

  public ValueDisplay(Literal value) {
    this.value = value;
    this.display = null;
  }

  public ValueDisplay(Literal value, String display) {
    this.value = value;
    this.display = display;
  }

  public ValueDisplay(String valueAndDisplay) {
    this.value = new Literal(valueAndDisplay);
    this.display = valueAndDisplay;
  }

  public static ValueDisplay fromSerialized(UFValueDisplay serialized) {
    if (serialized.getValue() == null) {
      throw new InvalidConfigException("Value is undefined");
    }
    return new ValueDisplay(Literal.fromSerialized(serialized.getValue()), serialized.getDisplay());
  }

  public Literal getValue() {
    return value;
  }

  public String getDisplay() {
    return display;
  }
}
