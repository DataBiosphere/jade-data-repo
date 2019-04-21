package bio.terra.service;

import bio.terra.category.Unit;
import bio.terra.service.exception.InvalidDrsIdException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@Category(Unit.class)
public class DrsIdTest {

    @Test
    public void testDrsId() {
        DrsId drsId = DrsId.builder()
            .dnsname("dns")
            .version("vv")
            .studyId("study")
            .datasetId("dataset")
            .fsObjectId("file")
            .build();

        assertThat("drsid constructor succeeds - dnsname",  drsId.getDnsname(), equalTo("dns"));
        assertThat("drsid constructor succeeds - version",  drsId.getVersion(), equalTo("vv"));
        assertThat("drsid constructor succeeds - studyId",  drsId.getStudyId(), equalTo("study"));
        assertThat("drsid constructor succeeds - datasetId",  drsId.getDatasetId(), equalTo("dataset"));
        assertThat("drsid constructor succeeds - fileId",  drsId.getFsObjectId(), equalTo("file"));

        assertThat("drsid toString works", drsId.toString(), equalTo("drs://dns/vv_study_dataset_file"));
    }

    @Test(expected = InvalidDrsIdException.class)
    public void testBadUri() {
        DrsId drsId = DrsId.builder()
            .dnsname("{[]}")
            .version("vv")
            .studyId("study")
            .datasetId("{[]}")
            .fsObjectId("file")
            .build();

        drsId.toString(); // should throw
    }

}
