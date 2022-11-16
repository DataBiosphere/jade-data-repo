package bio.terra.service.filedata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class DrsIdServiceTest {

  private static final String HOSTNAME = "myhost.org";

  private final ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration();

  private DrsIdService drsIdService;

  @Before
  public void setUp() throws Exception {
    applicationConfiguration.setDnsName(HOSTNAME);
    drsIdService = new DrsIdService(applicationConfiguration);
  }

  @Test
  public void testV1DrsIds() {
    UUID snapshotId = UUID.randomUUID();
    UUID fileId = UUID.randomUUID();
    assertThat(
        "v1 object id can be parsed",
        drsIdService.fromObjectId("v1_" + snapshotId + "_" + fileId),
        equalTo(new DrsId(HOSTNAME, "v1", snapshotId.toString(), fileId.toString())));
    assertThat(
        "v1 object id can be created",
        drsIdService.makeDrsId(new FSFile().fileId(fileId), snapshotId.toString()),
        equalTo(new DrsId(HOSTNAME, "v1", snapshotId.toString(), fileId.toString())));
    String drsUri = "drs://" + HOSTNAME + "/v1_" + snapshotId + "_" + fileId;
    assertThat(
        "v1 drs URI can be parsed",
        DrsIdService.fromUri(drsUri),
        equalTo(new DrsId(HOSTNAME, "v1", snapshotId.toString(), fileId.toString())));
    assertThat(
        "v1 drs URI can be parsed and returns the same URI",
        DrsIdService.fromUri(drsUri).toDrsUri(),
        equalTo(drsUri));
  }

  @Test
  public void testV2DrsIds() {
    UUID fileId = UUID.randomUUID();
    assertThat(
        "v2 object id can be parsed",
        drsIdService.fromObjectId("v2_" + fileId),
        equalTo(new DrsId(HOSTNAME, "v2", null, fileId.toString())));
    assertThat(
        "v2 object id can be created",
        drsIdService.makeDrsId(new FSFile().fileId(fileId)),
        equalTo(new DrsId(HOSTNAME, "v2", null, fileId.toString())));
    String drsUri = "drs://" + HOSTNAME + "/v2_" + fileId;
    assertThat(
        "v2 drs URI can be parsed",
        DrsIdService.fromUri(drsUri),
        equalTo(new DrsId(HOSTNAME, "v2", null, fileId.toString())));
    assertThat(
        "v2 drs URI can be parsed and returns the same URI",
        DrsIdService.fromUri(drsUri).toDrsUri(),
        equalTo(drsUri));
  }

  @Test
  public void testInvalidDrsIds() {
    UUID snapshotId = UUID.randomUUID();
    UUID fileId = UUID.randomUUID();
    assertThrows(
        "v3 object id cannot be parsed",
        InvalidDrsIdException.class,
        () -> drsIdService.fromObjectId("v3_" + snapshotId + "_" + fileId));

    assertThrows(
        "badly formed v1 object id cannot be parsed - no snapshot",
        InvalidDrsIdException.class,
        () -> drsIdService.fromObjectId("v1_" + fileId));

    assertThrows(
        "badly formed v1 object id cannot be parsed - extra field",
        InvalidDrsIdException.class,
        () -> drsIdService.fromObjectId("v1_" + snapshotId + "_" + fileId + "_foo"));

    assertThrows(
        "badly formed v2 object id cannot be parsed - has a snapshot",
        InvalidDrsIdException.class,
        () -> drsIdService.fromObjectId("v2_" + snapshotId + "_" + fileId));

    assertThrows(
        "badly formed uri - invalid protocol",
        InvalidDrsIdException.class,
        () -> DrsIdService.fromUri("https://notadrsurl.com"));
  }
}
