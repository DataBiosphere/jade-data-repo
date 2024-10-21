package bio.terra.service.snapshot.flight.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotDuosFlightUtilsTest {

  @Mock FlightContext context;

  @Test
  void testGetFirecloudGroup() {
    FlightMap workingMap = new FlightMap();
    when(context.getWorkingMap()).thenReturn(workingMap);

    assertNull(SnapshotDuosFlightUtils.getFirecloudGroup(context));

    DuosFirecloudGroupModel group = new DuosFirecloudGroupModel().id(UUID.randomUUID());
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, group);
    assertThat(SnapshotDuosFlightUtils.getFirecloudGroup(context), equalTo(group));
  }

  @Test
  void testGetDuosFirecloudGroupId() {
    assertNull(SnapshotDuosFlightUtils.getDuosFirecloudGroupId(null));
    assertNull(SnapshotDuosFlightUtils.getDuosFirecloudGroupId(new DuosFirecloudGroupModel()));

    UUID id = UUID.randomUUID();
    DuosFirecloudGroupModel groupWithId = new DuosFirecloudGroupModel().id(id);
    assertThat(SnapshotDuosFlightUtils.getDuosFirecloudGroupId(groupWithId), equalTo(id));
  }
}
