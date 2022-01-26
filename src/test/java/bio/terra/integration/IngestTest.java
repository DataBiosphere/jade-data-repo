package bio.terra.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;

import bio.terra.common.BQTestUtils;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.AccessInfoBigQueryModelTable;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.UpdateStrategyEnum;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.bigquery.TableResult;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
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

  @Autowired private JsonLoader jsonLoader;

  @Autowired private TestConfiguration testConfig;

  private UUID datasetId;
  private UUID profileId;

  @Before
  public void setup() throws Exception {
    super.setup();
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    dataRepoFixtures.addPolicyMember(
        steward(), profileId, IamRole.USER, custodian().getEmail(), IamResourceType.SPEND_PROFILE);

    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
    datasetId = datasetSummaryModel.getId();
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());
  }

  @After
  public void teardown() throws Exception {
    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  public void ingestAndUpdateParticipants() throws Exception {
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));

    IngestRequestModel updateIngestRequest =
        dataRepoFixtures
            .buildSimpleIngest("participant", "ingest-test/ingest-test-participant_upd1.json")
            .updateStrategy(UpdateStrategyEnum.REPLACE);
    IngestResponseModel updateIngestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, updateIngestRequest);
    assertThat(
        "correct updated participant row count", updateIngestResponse.getRowCount(), equalTo(3L));

    // Two of the rows should have overlapped so we should now see 6 rows
    // TODO: once the preview API GA and works for datasets, we should use that here
    DatasetModel dataset =
        dataRepoFixtures.getDataset(
            steward(), datasetId, List.of(DatasetRequestAccessIncludeModel.ACCESS_INFORMATION));
    BigQueryProject bigQueryProject =
        BigQueryProject.get(dataset.getAccessInformation().getBigQuery().getProjectId());
    AccessInfoBigQueryModelTable bqTableInfo =
        dataset.getAccessInformation().getBigQuery().getTables().stream()
            .filter(t -> t.getName().equals("participant"))
            .findFirst()
            .orElseThrow();
    // Note: the sample query is just a formatted select * query against the table
    TableResult bqQueryResult = bigQueryProject.query(bqTableInfo.getSampleQuery());

    assertThat("Expected number of rows are present", bqQueryResult.getTotalRows(), equalTo(6L));
    List<Map<String, Object>> results =
        BQTestUtils.mapToList(bqQueryResult, "id", "age", "children", "donated");

    List<Map<String, Object>> dataOrig =
        jsonLoader.loadObjectAsStream("ingest-test-participant.json", new TypeReference<>() {});
    List<Map<String, Object>> dataUpd =
        jsonLoader.loadObjectAsStream(
            "ingest-test-participant_upd1.json", new TypeReference<>() {});
    assertThat(
        "Values match",
        results,
        containsInAnyOrder(
            dataOrig.get(0), // ID = participant_1
            dataOrig.get(1), // ID = participant_2
            dataOrig.get(2), // ID = participant_3
            // Updated values
            dataUpd.get(0), // ID = participant_4
            dataUpd.get(1), // ID = participant_5
            dataUpd.get(2) // ID = participant_6
            ));
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
