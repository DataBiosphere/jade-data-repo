package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import bio.terra.common.category.Unit;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class ColumnTest {
  @Test
  void equals_compareIds() {
    assertThat("Columns with the default ID value are equal", new Column(), equalTo(new Column()));
    assertThat(
        "Columns with the same null ID are equal",
        new Column().id(null),
        equalTo(new Column().id(null)));
    UUID id = UUID.randomUUID();
    assertThat(
        "Columns with the same non-null ID are equal",
        new Column().id(id),
        equalTo(new Column().id(id)));
    assertThat(
        "Columns with different IDs are not equal",
        new Column().id(id),
        not(equalTo(new Column().id(UUID.randomUUID()))));
  }
}
