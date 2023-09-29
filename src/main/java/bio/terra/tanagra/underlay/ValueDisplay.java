package bio.terra.tanagra.underlay;

import bio.terra.tanagra.query.Literal;

public record ValueDisplay(Literal value, String display) {

  public ValueDisplay(Literal value) {
    this(value, null);
  }

  public ValueDisplay(String display) {
    this(new Literal(display), display);
  }
}
