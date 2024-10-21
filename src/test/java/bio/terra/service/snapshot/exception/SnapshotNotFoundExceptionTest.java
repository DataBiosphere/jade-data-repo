package bio.terra.service.snapshot.exception;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotFoundException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class SnapshotNotFoundExceptionTest {

  @Test
  void testSnapshotNotFoundException() {
    assertThat(
        "SnapshotNotFoundException is a NotFoundException",
        new SnapshotNotFoundException("Snapshot not found"),
        instanceOf(NotFoundException.class));
  }
}
