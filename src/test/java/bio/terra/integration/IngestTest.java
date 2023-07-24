package bio.terra.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;

import bio.terra.common.BQTestUtils;
import bio.terra.common.category.Integration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.AccessInfoBigQueryModelTable;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.UpdateStrategyEnum;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.TransactionCloseModel;
import bio.terra.model.TransactionCloseModel.ModeEnum;
import bio.terra.model.TransactionCreateModel;
import bio.terra.model.TransactionModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.bigquery.TableResult;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
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

  @Rule @Autowired public TestJobWatcher testWatcher;

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
    ingestAndUpdateParticipants(
        ingestFile -> {
          try {
            return dataRepoFixtures.buildSimpleIngest("participant", "ingest-test/" + ingestFile);
          } catch (Exception e) {
            throw new RuntimeException("Error building ingest request", e);
          }
        });
  }

  @Test
  public void ingestAndUpdateParticipantsViaDirectApi() throws Exception {
    ingestAndUpdateParticipants(
        ingestFile -> {
          try {
            List<Map<String, Object>> data =
                jsonLoader.loadObjectAsStream(ingestFile, new TypeReference<>() {});
            return dataRepoFixtures.buildSimpleIngest("participant", data);
          } catch (Exception e) {
            throw new RuntimeException("Error building ingest request", e);
          }
        });
  }

  private void ingestAndUpdateParticipants(Function<String, IngestRequestModel> ingestCreator)
      throws Exception {
    IngestRequestModel ingestRequest = ingestCreator.apply("ingest-test-participant.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));

    IngestRequestModel updateIngestRequest =
        ingestCreator
            .apply("ingest-test-update-participant.json")
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
            "ingest-test-update-participant.json", new TypeReference<>() {});
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
  public void ingestAndUpdateParticipantsWithTransaction() throws Exception {
    TransactionModel transaction =
        dataRepoFixtures.openTransaction(
            steward(), datasetId, new TransactionCreateModel().description("foo"));
    UUID badTransaction = UUID.randomUUID();
    IngestRequestModel ingestRequest =
        dataRepoFixtures
            .buildSimpleIngest("participant", "ingest-test/ingest-test-participant.json")
            // Bogus transaction should fail
            .transactionId(badTransaction);

    // Should fail with unrecognized transaction
    DataRepoResponse<IngestResponseModel> badIngestResponse =
        dataRepoFixtures.ingestJsonDataRaw(steward(), datasetId, ingestRequest);
    assertThat(
        "Could not find transaction",
        badIngestResponse.getStatusCode(),
        equalTo(HttpStatus.NOT_FOUND));

    assertThat(
        "Error message looks reasonable",
        badIngestResponse.getErrorObject().get().getMessage(),
        startsWith(String.format("Transaction %s not found in dataset", badTransaction)));

    ingestRequest.transactionId(transaction.getId());

    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));

    IngestRequestModel updateIngestRequest =
        dataRepoFixtures
            .buildSimpleIngest("participant", "ingest-test/ingest-test-update-participant.json")
            .updateStrategy(UpdateStrategyEnum.REPLACE)
            .transactionId(transaction.getId());
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

    assertThat("No commit so rows aren't there", bqQueryResult.getTotalRows(), equalTo(0L));

    // Commit and rows should now be present
    dataRepoFixtures.closeTransaction(
        steward(),
        datasetId,
        transaction.getId(),
        new TransactionCloseModel().mode(ModeEnum.COMMIT));

    TableResult bqQueryResultCommitted = bigQueryProject.query(bqTableInfo.getSampleQuery());
    assertThat("Committed rows are there", bqQueryResultCommitted.getTotalRows(), equalTo(6L));
  }

  @Test
  public void ingestJsonData() throws Exception {
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant-with-json-data.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    List<String> vals =
        dataRepoFixtures.getColumnValues(steward(), datasetId, "participant", "jsonData", 5);
    assertThat(
        "correct jsonData values",
        vals,
        containsInAnyOrder(
            containsString("{\"testField\":\"Bye-bye\"}"),
            containsString("{\"testField\":\"Hello\"}")));
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
        equalTo(HttpStatus.FORBIDDEN));
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
        dataRepoClient.waitForResponse(steward(), ingestJobResponse, new TypeReference<>() {});
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
        dataRepoClient.waitForResponse(steward(), ingestJobResponse, new TypeReference<>() {});
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
        dataRepoClient.waitForResponse(steward(), ingestJobResponse, new TypeReference<>() {});
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
        dataRepoClient.waitForResponse(steward(), ingestJobResponse, new TypeReference<>() {});
    assertThat("ingest failed", ingestResponse.getStatusCode(), equalTo(HttpStatus.BAD_REQUEST));
    assertThat(
        "failure is explained",
        ingestResponse.getErrorObject().orElseThrow(IllegalStateException::new).getErrorDetail(),
        hasItem(containsString("too many errors")));
  }

  @Test
  public void ingestMergeHappyPathTest() throws Exception {
    DatasetModel dataset =
        dataRepoFixtures.getDataset(
            steward(), datasetId, List.of(DatasetRequestAccessIncludeModel.ACCESS_INFORMATION));
    // -------- Simple ingest with 7 rows --------
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(7L));
    assertSampleTableIdColumnRemainsUnchanged(dataset);
    // Original ingest request should include value 'sample7' for column 'derived_from'
    dataRepoFixtures.assertColumnTextValueCount(
        steward(), datasetId, "sample", "derived_from", "sample7", 1);
    // Test column stats endpoint's handling of array columns
    dataRepoFixtures.assertColumnTextValueCount(
        steward(), datasetId, "sample", "participant_ids", "participant_1", 1);

    // Rows ingested via merge should not increase the existing live row count.
    IngestRequestModel mergeIngestRequest =
        dataRepoFixtures
            .buildSimpleIngest("sample", "ingest-test/merge/ingest-test-sample-merge.json")
            .updateStrategy(UpdateStrategyEnum.MERGE);
    IngestResponseModel mergeIngestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, mergeIngestRequest);
    assertThat("correct merge sample row count", mergeIngestResponse.getRowCount(), equalTo(2L));
    assertSampleTableIdColumnRemainsUnchanged(dataset);
    // We cannot "null-out" a value in a merge ingest request
    // so the value remains 'sample7' for the 'derived_from' column despite being set to null in the
    // request
    dataRepoFixtures.assertColumnTextValueCount(
        steward(), datasetId, "sample", "derived_from", "sample7", 1);

    // -------- Updating the same row again via merge ingest should succeed--------
    IngestRequestModel mergeAgainIngestRequest =
        dataRepoFixtures
            .buildSimpleIngest("sample", "ingest-test/merge/ingest-test-sample-merge-again.json")
            .updateStrategy(UpdateStrategyEnum.MERGE);
    IngestResponseModel mergeAgainIngestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, mergeAgainIngestRequest);
    assertThat(
        "correct merge again sample row count",
        mergeAgainIngestResponse.getRowCount(),
        equalTo(1L));
    assertSampleTableIdColumnRemainsUnchanged(dataset);
  }

  private void assertSampleTableIdColumnRemainsUnchanged(DatasetModel dataset) throws Exception {
    int expectedNumRows = 7;
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "sample", expectedNumRows);
    List<String> actualValues =
        dataRepoFixtures.retrieveColumnTextValues(steward(), datasetId, "sample", "id");
    assertThat(
        "Expected values returned from column stats endpoint for sample id table",
        actualValues,
        containsInAnyOrder(
            "sample1", "sample2", "sample3", "sample4", "sample5", "sample6", "sample7"));
  }

  @Test
  public void ingestMergeNoTargetPKTest() throws Exception {
    IngestRequestModel mergeIngestRequest =
        dataRepoFixtures
            .buildSimpleIngest("file", "ingest-test/ingest-test-file.json")
            .updateStrategy(UpdateStrategyEnum.MERGE);

    DataRepoResponse<JobModel> mergeIngestJobResponse =
        dataRepoFixtures.ingestJsonDataLaunch(steward(), datasetId, mergeIngestRequest);
    DataRepoResponse<IngestResponseModel> mergeIngestResponse =
        dataRepoClient.waitForResponse(steward(), mergeIngestJobResponse, new TypeReference<>() {});

    assertThat(
        "ingest failed", mergeIngestResponse.getStatusCode(), equalTo(HttpStatus.BAD_REQUEST));
    assertThat(
        "failure is explained",
        mergeIngestResponse.getErrorObject().get().getMessage(),
        equalTo("Cannot ingest to a table without a primary key defined."));
  }

  @Test
  public void ingestMergeRowsMissingPKsTest() throws Exception {
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(7L));

    IngestRequestModel mergeIngestRequest =
        dataRepoFixtures
            .buildSimpleIngest(
                "sample", "ingest-test/merge/ingest-test-sample-merge-missing-pks.json")
            .updateStrategy(UpdateStrategyEnum.MERGE);
    DataRepoResponse<JobModel> mergeIngestJobResponse =
        dataRepoFixtures.ingestJsonDataLaunch(steward(), datasetId, mergeIngestRequest);
    DataRepoResponse<IngestResponseModel> mergeIngestResponse =
        dataRepoClient.waitForResponse(steward(), mergeIngestJobResponse, new TypeReference<>() {});

    assertThat(
        "ingest failed", mergeIngestResponse.getStatusCode(), equalTo(HttpStatus.BAD_REQUEST));
    assertThat(
        "failure is explained",
        mergeIngestResponse.getErrorObject().get().getMessage(),
        containsString("Ingest failed"));
    assertThat(
        "primary key specification is enforced",
        mergeIngestResponse.getErrorObject().get().getErrorDetail(),
        hasItem(containsString("Missing required field: id")));
  }

  @Test
  public void ingestMergeRowsDuplicatePKsTest() throws Exception {
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(7L));

    IngestRequestModel mergeIngestRequest =
        dataRepoFixtures
            .buildSimpleIngest(
                "sample", "ingest-test/merge/ingest-test-sample-merge-duplicate-pks.json")
            .updateStrategy(UpdateStrategyEnum.MERGE);
    DataRepoResponse<JobModel> mergeIngestJobResponse =
        dataRepoFixtures.ingestJsonDataLaunch(steward(), datasetId, mergeIngestRequest);
    DataRepoResponse<IngestResponseModel> mergeIngestResponse =
        dataRepoClient.waitForResponse(steward(), mergeIngestJobResponse, new TypeReference<>() {});

    assertThat(
        "ingest failed", mergeIngestResponse.getStatusCode(), equalTo(HttpStatus.BAD_REQUEST));
    assertThat(
        "failure is explained",
        mergeIngestResponse.getErrorObject().get().getMessage(),
        containsString("Duplicate primary keys identified"));
    assertThat(
        "all duplicate primary keys are found in error details",
        mergeIngestResponse.getErrorObject().get().getErrorDetail(),
        containsInAnyOrder(containsString("sample1")));
  }

  @Test
  public void ingestMergeMismatchedWithTargetTest() throws Exception {
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));

    IngestRequestModel ingestWithDupesRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-update-participant.json");
    IngestResponseModel ingestWithDupesResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestWithDupesRequest);
    assertThat(
        "correct participant new row count", ingestWithDupesResponse.getRowCount(), equalTo(3L));

    IngestRequestModel mergeIngestRequest =
        dataRepoFixtures
            .buildSimpleIngest(
                "participant", "ingest-test/merge/ingest-test-participant-merge-mismatched.json")
            .updateStrategy(UpdateStrategyEnum.MERGE);
    DataRepoResponse<JobModel> mergeIngestJobResponse =
        dataRepoFixtures.ingestJsonDataLaunch(steward(), datasetId, mergeIngestRequest);
    DataRepoResponse<IngestResponseModel> mergeIngestResponse =
        dataRepoClient.waitForResponse(steward(), mergeIngestJobResponse, new TypeReference<>() {});

    assertThat(
        "ingest failed", mergeIngestResponse.getStatusCode(), equalTo(HttpStatus.BAD_REQUEST));
    assertThat(
        "failure is explained",
        mergeIngestResponse.getErrorObject().get().getMessage(),
        containsString("merge record(s) did not resolve to a single target record"));
    assertThat(
        "all primary keys without single target table matches are found in error details",
        mergeIngestResponse.getErrorObject().get().getErrorDetail(),
        containsInAnyOrder(containsString("participant_4"), containsString("participant_100")));
  }
}
