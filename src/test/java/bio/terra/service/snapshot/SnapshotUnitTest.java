package bio.terra.service.snapshot;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class SnapshotUnitTest {
  @Test
  void isSnapshot() {
    Snapshot snapshot = new Snapshot();
    assertTrue(snapshot.isSnapshot());
  }

  @Test
  void isDataset() {
    Snapshot snapshot = new Snapshot();
    assertFalse(snapshot.isDataset());
  }
}
