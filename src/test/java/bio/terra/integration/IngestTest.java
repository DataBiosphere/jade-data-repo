package bio.terra.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;

import bio.terra.common.BQTestUtils;
import bio.terra.common.auth.Users;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration.User;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.AccessInfoBigQueryModelTable;
import bio.terra.model.DatasetDataModel;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = IntegrationTestConfiguration.class)
@ActiveProfiles({"google", "integrationtest"})
@Tag(Integration.TAG)
@Execution(ExecutionMode.CONCURRENT)
class IngestTest {

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private DataRepoClient dataRepoClient;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private Users users;

  private final ThreadLocal<Users.TestUsers> testUsers =
      ThreadLocal.withInitial(() -> users.testUsers());

  private final ThreadLocal<UUID> tlDatasetId = new ThreadLocal<>();
  private final ThreadLocal<UUID> tlProfileId = new ThreadLocal<>();

  private User steward() {
    return testUsers.get().steward();
  }

  private User custodian() {
    return testUsers.get().custodian();
  }

  private User reader() {
    return testUsers.get().reader();
  }

  @BeforeEach
  public void setup() throws Exception {
    var profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    tlProfileId.set(profileId);
    dataRepoFixtures.addPolicyMember(
        steward(), profileId, IamRole.USER, custodian().email(), IamResourceType.SPEND_PROFILE);

    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
    var datasetId = datasetSummaryModel.getId();
    tlDatasetId.set(datasetId);
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().email());
  }

  @AfterEach
  public void teardown() throws Exception {
    if (tlDatasetId.get() != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), tlDatasetId.get());
    }

    if (tlProfileId.get() != null) {
      dataRepoFixtures.deleteProfileLog(steward(), tlProfileId.get());
    }
  }

  @Test
  void ingestAndUpdateParticipants() throws Exception {
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
  void ingestAndUpdateParticipantsViaDirectApi() throws Exception {
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
    var datasetId = tlDatasetId.get();
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
  void ingestAndUpdateParticipantsWithTransaction() throws Exception {
    var datasetId = tlDatasetId.get();
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
        badIngestResponse.getErrorObject().orElseThrow().getMessage(),
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
  void ingestJsonData() throws Exception {
    var datasetId = tlDatasetId.get();
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant-with-json-data.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);

    DatasetDataModel data =
        dataRepoFixtures.retrieveDatasetData(steward(), datasetId, "participant", 0, 6, null);
    assertThat("correct participant row count", data.getFilteredRowCount(), equalTo(5));
    DatasetDataModel filteredData =
        dataRepoFixtures.retrieveDatasetData(
            steward(),
            datasetId,
            "participant",
            0,
            6,
            "CAST(JSON_EXTRACT_SCALAR(jsonData, '$.numericField') AS INT64) > 5");
    assertThat("correct filtered number of rows", filteredData.getFilteredRowCount(), equalTo(2));
  }

  @Test
  void ingestWildcardSuffix() throws Exception {
    var datasetId = tlDatasetId.get();
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/wildcard/ingest-test-participant*");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(7L));
  }

  @Test
  void ingestWildcardMiddle() throws Exception {
    var datasetId = tlDatasetId.get();
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/wildcard/ingest-test-p*t.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(6L));
  }

  @Test
  void ingestAuthorizationTest() throws Exception {
    var datasetId = tlDatasetId.get();
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
  void ingestAppendNoPkTest() throws Exception {
    var datasetId = tlDatasetId.get();
    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest("file", "ingest-test/ingest-test-file.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
    assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));

    ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
    assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));
  }

  @Test
  void ingestBadPathTest() throws Exception {
    var datasetId = tlDatasetId.get();
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
  void ingestEmptyPatternTest() throws Exception {
    var datasetId = tlDatasetId.get();
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
  void ingestSingleFileMalformedTest() throws Exception {
    var datasetId = tlDatasetId.get();
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
  void ingestWildcardMalformedTest() throws Exception {
    var datasetId = tlDatasetId.get();
    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest("file", "ingest-test/wildcard/ingest-test-p*.json");
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
  void ingestMergeHappyPathTest() throws Exception {
    var datasetId = tlDatasetId.get();
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
    var datasetId = dataset.getId();
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
  void ingestMergeNoTargetPKTest() throws Exception {
    var datasetId = tlDatasetId.get();
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
        mergeIngestResponse.getErrorObject().orElseThrow().getMessage(),
        equalTo("Cannot ingest to a table without a primary key defined."));
  }

  @Test
  void ingestMergeRowsMissingPKsTest() throws Exception {
    var datasetId = tlDatasetId.get();
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
        mergeIngestResponse.getErrorObject().orElseThrow().getMessage(),
        containsString("Ingest failed"));
    assertThat(
        "primary key specification is enforced",
        mergeIngestResponse.getErrorObject().get().getErrorDetail(),
        hasItem(containsString("Missing required field: id")));
  }

  @Test
  void ingestMergeRowsDuplicatePKsTest() throws Exception {
    var datasetId = tlDatasetId.get();
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
        mergeIngestResponse.getErrorObject().orElseThrow().getMessage(),
        containsString("Duplicate primary keys identified"));
    assertThat(
        "all duplicate primary keys are found in error details",
        mergeIngestResponse.getErrorObject().orElseThrow().getErrorDetail(),
        containsInAnyOrder(containsString("sample1")));
  }

  @Test
  void ingestMergeMismatchedWithTargetTest() throws Exception {
    var datasetId = tlDatasetId.get();
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
        mergeIngestResponse.getErrorObject().orElseThrow().getMessage(),
        containsString("merge record(s) did not resolve to a single target record"));
    assertThat(
        "all primary keys without single target table matches are found in error details",
        mergeIngestResponse.getErrorObject().get().getErrorDetail(),
        containsInAnyOrder(containsString("participant_4"), containsString("participant_100")));
  }
}
