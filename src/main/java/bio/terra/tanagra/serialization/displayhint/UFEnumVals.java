package bio.terra.tanagra.serialization.displayhint;

import bio.terra.tanagra.serialization.UFDisplayHint;
import bio.terra.tanagra.underlay.displayhint.EnumVals;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.List;
import java.util.stream.Collectors;

/**
 * External representation of an enum display hint.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFEnumVals.Builder.class)
public class UFEnumVals extends UFDisplayHint {
  private final List<UFEnumVal> enumVals;

  public UFEnumVals(EnumVals displayHint) {
    super(displayHint);
    this.enumVals =
        displayHint.getEnumValsList().stream()
            .map(ev -> new UFEnumVal(ev))
            .collect(Collectors.toList());
  }

  private UFEnumVals(Builder builder) {
    super(builder);
    this.enumVals = builder.enumVals;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder extends UFDisplayHint.Builder {
    private List<UFEnumVal> enumVals;

    public Builder enumVals(List<UFEnumVal> enumVals) {
      this.enumVals = enumVals;
      return this;
    }

    @Override
    public UFEnumVals build() {
      return new UFEnumVals(this);
    }
  }

  /** Deserialize to the internal representation of the display hint. */
  @Override
  public EnumVals deserializeToInternal() {
    return EnumVals.fromSerialized(this);
  }

  public List<UFEnumVal> getEnumVals() {
    return enumVals;
  }
}
