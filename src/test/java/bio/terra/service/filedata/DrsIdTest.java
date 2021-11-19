package bio.terra.service.filedata;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.common.category.Unit;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DrsIdTest {

  @Test
  public void testDrsId() {
    DrsId drsId =
        DrsId.builder()
            .dnsname("dns")
            .version("vv")
            .snapshotId("snapshot")
            .fsObjectId("file")
            .build();

    assertThat("drsid constructor succeeds - dnsname", drsId.getDnsname(), equalTo("dns"));
    assertThat("drsid constructor succeeds - version", drsId.getVersion(), equalTo("vv"));
    assertThat(
        "drsid constructor succeeds - snapshotId", drsId.getSnapshotId(), equalTo("snapshot"));
    assertThat("drsid constructor succeeds - fileId", drsId.getFsObjectId(), equalTo("file"));
    assertThat("drsid toDrsUri works", drsId.toDrsUri(), equalTo("drs://dns/vv_snapshot_file"));
    assertThat("drsid toObjectId works", drsId.toDrsObjectId(), equalTo("vv_snapshot_file"));
  }

  @Test(expected = InvalidDrsIdException.class)
  public void testBadUri() {
    DrsId drsId =
        DrsId.builder().dnsname("{[]}").version("vv").snapshotId("{[]}").fsObjectId("file").build();

    drsId.toDrsUri(); // should throw
  }
}
