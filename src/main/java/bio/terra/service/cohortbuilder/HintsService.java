package bio.terra.service.cohortbuilder;

import bio.terra.model.Attribute;
import bio.terra.model.DataType;
import bio.terra.model.DisplayHint;
import bio.terra.model.DisplayHintDisplayHint;
import bio.terra.model.DisplayHintEnum;
import bio.terra.model.DisplayHintEnumEnumHintValues;
import bio.terra.model.DisplayHintList;
import bio.terra.model.DisplayHintNumericRange;
import bio.terra.model.HintQuery;
import bio.terra.model.Literal;
import bio.terra.model.LiteralValueUnion;
import bio.terra.model.ValueDisplay;
import java.util.Arrays;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class HintsService {
  public DisplayHintList queryHints(String entityId, HintQuery hintQuery) {
    return new DisplayHintList()
        .addDisplayHintsItem(
            new DisplayHint()
                .attribute(
                    new Attribute()
                        .name("gender")
                        .type(Attribute.TypeEnum.KEY_AND_DISPLAY)
                        .dataType(DataType.INT64))
                .displayHint(
                    new DisplayHintDisplayHint()
                        .enumHint(
                            new DisplayHintEnum()
                                .enumHintValues(
                                    List.of(
                                        new DisplayHintEnumEnumHintValues()
                                            .enumVal(
                                                new ValueDisplay()
                                                    .display("FEMALE")
                                                    .value(
                                                        new Literal()
                                                            .dataType(DataType.INT64)
                                                            .valueUnion(
                                                                new LiteralValueUnion()
                                                                    .int64Val(8532L))))
                                            .count(1292861),
                                        new DisplayHintEnumEnumHintValues()
                                            .enumVal(
                                                new ValueDisplay()
                                                    .display("MALE")
                                                    .value(
                                                        new Literal()
                                                            .dataType(DataType.INT64)
                                                            .valueUnion(
                                                                new LiteralValueUnion()
                                                                    .int64Val(8507L))))
                                            .count(1033995))))))
        .addDisplayHintsItem(
            new DisplayHint()
                .attribute(
                    new Attribute()
                        .name("race")
                        .type(Attribute.TypeEnum.KEY_AND_DISPLAY)
                        .dataType(DataType.INT64))
                .displayHint(
                    new DisplayHintDisplayHint()
                        .enumHint(
                            new DisplayHintEnum()
                                .enumHintValues(
                                    Arrays.asList(
                                        new DisplayHintEnumEnumHintValues()
                                            .enumVal(
                                                new ValueDisplay()
                                                    .display("Black or African American")
                                                    .value(
                                                        new Literal()
                                                            .dataType(DataType.INT64)
                                                            .valueUnion(
                                                                new LiteralValueUnion()
                                                                    .int64Val(8516L))))
                                            .count(247723),
                                        new DisplayHintEnumEnumHintValues()
                                            .enumVal(
                                                new ValueDisplay()
                                                    .display("No matching concept")
                                                    .value(
                                                        new Literal()
                                                            .dataType(DataType.INT64)
                                                            .valueUnion(
                                                                new LiteralValueUnion()
                                                                    .int64Val(0L))))
                                            .count(152425),
                                        new DisplayHintEnumEnumHintValues()
                                            .enumVal(
                                                new ValueDisplay()
                                                    .display("White")
                                                    .value(
                                                        new Literal()
                                                            .dataType(DataType.INT64)
                                                            .valueUnion(
                                                                new LiteralValueUnion()
                                                                    .int64Val(8527L))))
                                            .count(1926708))))))
        .addDisplayHintsItem(
            new DisplayHint()
                .attribute(
                    new Attribute()
                        .name("ethnicity")
                        .type(Attribute.TypeEnum.KEY_AND_DISPLAY)
                        .dataType(DataType.INT64))
                .displayHint(
                    new DisplayHintDisplayHint()
                        .enumHint(
                            new DisplayHintEnum()
                                .enumHintValues(
                                    Arrays.asList(
                                        new DisplayHintEnumEnumHintValues()
                                            .enumVal(
                                                new ValueDisplay()
                                                    .display("Hispanic or Latino")
                                                    .value(
                                                        new Literal()
                                                            .dataType(DataType.INT64)
                                                            .valueUnion(
                                                                new LiteralValueUnion()
                                                                    .int64Val(38003563L))))
                                            .count(54453),
                                        new DisplayHintEnumEnumHintValues()
                                            .enumVal(
                                                new ValueDisplay()
                                                    .display("Not Hispanic or Latino")
                                                    .value(
                                                        new Literal()
                                                            .dataType(DataType.INT64)
                                                            .valueUnion(
                                                                new LiteralValueUnion()
                                                                    .int64Val(38003564L))))
                                            .count(2272403))))))
        .addDisplayHintsItem(
            new DisplayHint()
                .attribute(
                    new Attribute()
                        .name("year_of_birth")
                        .type(Attribute.TypeEnum.SIMPLE)
                        .dataType(DataType.INT64))
                .displayHint(
                    new DisplayHintDisplayHint()
                        .numericRangeHint(
                            new DisplayHintNumericRange().min(1909.0D).max(1983.0D))));
  }
}
