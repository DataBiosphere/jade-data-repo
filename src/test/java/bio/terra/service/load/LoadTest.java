package bio.terra.service.load;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import bio.terra.common.category.Unit;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class LoadTest {
  private static final String FLIGHT_ID_1 = "flight-id-1";
  private static final String FLIGHT_ID_2 = "flight-id-2";
  private static final UUID DATASET_ID_1 = UUID.randomUUID();
  private static final UUID DATASET_ID_2 = UUID.randomUUID();

  @Test
  void isLockedBy() {
    Load load = new Load(UUID.randomUUID(), "a-load-tag", FLIGHT_ID_1, DATASET_ID_1);
    assertTrue(load.isLockedBy(FLIGHT_ID_1, DATASET_ID_1));
    assertFalse(load.isLockedBy(FLIGHT_ID_1, DATASET_ID_2));
    assertFalse(load.isLockedBy(FLIGHT_ID_2, DATASET_ID_1));
    assertFalse(load.isLockedBy(FLIGHT_ID_2, DATASET_ID_2));
  }
}
