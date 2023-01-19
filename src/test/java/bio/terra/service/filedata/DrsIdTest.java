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
  public void testDrsIdV1() {
    DrsId drsId =
        DrsId.builder()
            .dnsname("dns")
            .version("v1")
            .snapshotId("snapshot")
            .fsObjectId("file")
            .build();

    assertThat("drsid constructor succeeds - dnsname", drsId.getDnsname(), equalTo("dns"));
    assertThat("drsid constructor succeeds - version", drsId.getVersion(), equalTo("v1"));
    assertThat(
        "drsid constructor succeeds - snapshotId", drsId.getSnapshotId(), equalTo("snapshot"));
    assertThat("drsid constructor succeeds - fileId", drsId.getFsObjectId(), equalTo("file"));
    assertThat("drsid toDrsUri works", drsId.toDrsUri(), equalTo("drs://dns/v1_snapshot_file"));
    assertThat("drsid toObjectId works", drsId.toDrsObjectId(), equalTo("v1_snapshot_file"));
  }

  @Test
  public void testDrsIdV2() {
    DrsId drsId = DrsId.builder().dnsname("dns").version("v2").fsObjectId("file").build();

    assertThat("drsid constructor succeeds - dnsname", drsId.getDnsname(), equalTo("dns"));
    assertThat("drsid constructor succeeds - version", drsId.getVersion(), equalTo("v2"));
    assertThat("drsid constructor succeeds - fileId", drsId.getFsObjectId(), equalTo("file"));
    assertThat("drsid toDrsUri works", drsId.toDrsUri(), equalTo("drs://dns/v2_file"));
    assertThat("drsid toObjectId works", drsId.toDrsObjectId(), equalTo("v2_file"));
  }

  @Test(expected = InvalidDrsIdException.class)
  public void testBadUri() {
    DrsId drsId =
        DrsId.builder().dnsname("{[]}").version("vv").snapshotId("{[]}").fsObjectId("file").build();

    drsId.toDrsUri(); // should throw
  }
}
