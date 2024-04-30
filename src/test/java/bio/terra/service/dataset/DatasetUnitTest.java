package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class DatasetUnitTest {
  @Test
  void isSnapshot() {
    assertThat("not a snapshot", !new Dataset().isSnapshot());
  }

  @Test
  void isDataset() {
    assertThat("is a dataset", new Dataset().isDataset());
  }
}
