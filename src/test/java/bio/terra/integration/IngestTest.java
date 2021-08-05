package bio.terra.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;

import bio.terra.common.category.Integration;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class IngestTest extends UsersBase {

  @Autowired private DataRepoFixtures dataRepoFixtures;

  @Autowired private DataRepoClient dataRepoClient;

  private DatasetSummaryModel datasetSummaryModel;
  private UUID datasetId;
  private UUID profileId;
  private final List<UUID> createdSnapshotIds = new ArrayList<>();

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    dataRepoFixtures.addPolicyMember(
        steward(), profileId, IamRole.USER, custodian().getEmail(), IamResourceType.SPEND_PROFILE);

    datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
    datasetId = datasetSummaryModel.getId();
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());
  }

  @After
  public void teardown() throws Exception {
    for (UUID snapshotId : createdSnapshotIds) {
      dataRepoFixtures.deleteSnapshotLog(custodian(), snapshotId);
    }

    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Ignore // subset of the snapshot test; not worth running everytime, but useful for debugging
  @Test
  public void ingestParticipants() throws Exception {
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));
  }

  @Test
  public void ingestWildcardSuffix() throws Exception {
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest("participant", "ingest-test/ingest-test-participant*");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(7L));
  }

  @Test
  public void ingestWildcardMiddle() throws Exception {
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest("participant", "ingest-test/ingest-test-p*t.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(6L));
  }

  @Test
  public void ingestBuildSnapshot() throws Exception {
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));

    ingestRequest =
        dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(7L));

    ingestRequest = dataRepoFixtures.buildSimpleIngest("file", "ingest-test/ingest-test-file.json");
    ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshot(
            custodian(), datasetSummaryModel.getName(), profileId, "ingest-test-snapshot.json");
    createdSnapshotIds.add(snapshotSummary.getId());
  }

  @Test
  public void ingestAuthorizationTest() throws Exception {
    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    IngestResponseModel ingestCustodianResp =
        dataRepoFixtures.ingestJsonData(custodian(), datasetId, request);
    assertThat("Custodian was able to ingest", ingestCustodianResp.getRowCount(), greaterThan(0L));
    DataRepoResponse<JobModel> ingestReadResp =
        dataRepoFixtures.ingestJsonDataLaunch(reader(), datasetId, request);
    assertThat(
        "Reader is not authorized to ingest data",
        ingestReadResp.getStatusCode(),
        equalTo(HttpStatus.UNAUTHORIZED));
  }

  @Test
  public void ingestAppendNoPkTest() throws Exception {
    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest("file", "ingest-test/ingest-test-file.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
    assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));

    ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
    assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));
  }

  @Test
  public void ingestBadPathTest() throws Exception {
    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest("file", "totally-legit-file.json");
    DataRepoResponse<JobModel> ingestJobResponse =
        dataRepoFixtures.ingestJsonDataLaunch(steward(), datasetId, request);
    DataRepoResponse<IngestResponseModel> ingestResponse =
        dataRepoClient.waitForResponse(steward(), ingestJobResponse, IngestResponseModel.class);
    assertThat("ingest failed", ingestResponse.getStatusCode(), equalTo(HttpStatus.NOT_FOUND));
    assertThat(
        "failure is explained",
        ingestResponse.getErrorObject().orElseThrow(IllegalStateException::new).getMessage(),
        containsString("not found"));
  }

  @Test
  public void ingestEmptyPatternTest() throws Exception {
    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest("file", "prefix-matching-nothing/*");
    DataRepoResponse<JobModel> ingestJobResponse =
        dataRepoFixtures.ingestJsonDataLaunch(steward(), datasetId, request);
    DataRepoResponse<IngestResponseModel> ingestResponse =
        dataRepoClient.waitForResponse(steward(), ingestJobResponse, IngestResponseModel.class);
    assertThat("ingest failed", ingestResponse.getStatusCode(), equalTo(HttpStatus.NOT_FOUND));
    assertThat(
        "failure is explained",
        ingestResponse.getErrorObject().orElseThrow(IllegalStateException::new).getMessage(),
        containsString("not found"));
  }

  @Test
  public void ingestBadBucketPatternTest() throws Exception {
    IngestRequestModel request =
        new IngestRequestModel()
            .table("file")
            .format(IngestRequestModel.FormatEnum.JSON)
            .path("gs://bucket*pattern/some-file.json");
    DataRepoResponse<JobModel> ingestJobResponse =
        dataRepoFixtures.ingestJsonDataLaunch(steward(), datasetId, request);
    DataRepoResponse<IngestResponseModel> ingestResponse =
        dataRepoClient.waitForResponse(steward(), ingestJobResponse, IngestResponseModel.class);
    assertThat("ingest failed", ingestResponse.getStatusCode(), equalTo(HttpStatus.BAD_REQUEST));
    assertThat(
        "failure is explained",
        ingestResponse.getErrorObject().orElseThrow(IllegalStateException::new).getMessage(),
        containsString("not supported"));
  }

  @Test
  public void ingestBadMultiWildcardTest() throws Exception {
    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest("file", "ingest-prefix/*/ingest/suffix/*.json");
    DataRepoResponse<JobModel> ingestJobResponse =
        dataRepoFixtures.ingestJsonDataLaunch(steward(), datasetId, request);
    DataRepoResponse<IngestResponseModel> ingestResponse =
        dataRepoClient.waitForResponse(steward(), ingestJobResponse, IngestResponseModel.class);
    assertThat("ingest failed", ingestResponse.getStatusCode(), equalTo(HttpStatus.BAD_REQUEST));
    assertThat(
        "failure is explained",
        ingestResponse.getErrorObject().orElseThrow(IllegalStateException::new).getMessage(),
        containsString("not supported"));
  }

  @Test
  public void ingestSingleFileMalformedTest() throws Exception {
    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest(
            "file", "ingest-test/ingest-test-prtcpnt-malformed.json");
    DataRepoResponse<JobModel> ingestJobResponse =
        dataRepoFixtures.ingestJsonDataLaunch(steward(), datasetId, request);
    DataRepoResponse<IngestResponseModel> ingestResponse =
        dataRepoClient.waitForResponse(steward(), ingestJobResponse, IngestResponseModel.class);
    assertThat("ingest failed", ingestResponse.getStatusCode(), equalTo(HttpStatus.BAD_REQUEST));
    assertThat(
        "failure is explained",
        ingestResponse.getErrorObject().orElseThrow(IllegalStateException::new).getErrorDetail(),
        hasItem(containsString("too many errors")));
  }

  @Test
  public void ingestWildcardMalformedTest() throws Exception {
    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest("file", "ingest-test/ingest-test-p*.json");
    DataRepoResponse<JobModel> ingestJobResponse =
        dataRepoFixtures.ingestJsonDataLaunch(steward(), datasetId, request);
    DataRepoResponse<IngestResponseModel> ingestResponse =
        dataRepoClient.waitForResponse(steward(), ingestJobResponse, IngestResponseModel.class);
    assertThat("ingest failed", ingestResponse.getStatusCode(), equalTo(HttpStatus.BAD_REQUEST));
    assertThat(
        "failure is explained",
        ingestResponse.getErrorObject().orElseThrow(IllegalStateException::new).getErrorDetail(),
        hasItem(containsString("too many errors")));
  }
}
