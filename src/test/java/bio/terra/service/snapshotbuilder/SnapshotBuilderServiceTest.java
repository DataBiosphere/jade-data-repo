package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.model.EnumerateSnapshotAccessRequestItem;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import bio.terra.service.dataset.DatasetService;
import com.google.cloud.bigquery.EmptyTableResult;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotBuilderServiceTest {
  @Mock private SnapshotRequestDao snapshotRequestDao;
  private SnapshotBuilderService snapshotBuilderService;
  @Mock private DatasetService datasetService;
  @Mock private BigQueryDatasetPdao bigQueryDatasetPdao;
  @Mock private AzureSynapsePdao azureSynapsePdao;

  @BeforeEach
  public void beforeEach() {
    snapshotBuilderService =
        new SnapshotBuilderService(
            snapshotRequestDao, datasetService, bigQueryDatasetPdao, azureSynapsePdao);
  }

  @Test
  void createSnapshotRequest() {
    UUID datasetId = UUID.randomUUID();
    String email = "user@gmail.com";
    SnapshotAccessRequestResponse response = new SnapshotAccessRequestResponse();
    when(snapshotRequestDao.create(
            datasetId, SnapshotBuilderTestData.createSnapshotAccessRequest(), email))
        .thenReturn(response);

    assertThat(
        "createSnapshotRequest returns the expected response",
        snapshotBuilderService.createSnapshotRequest(
            datasetId, SnapshotBuilderTestData.createSnapshotAccessRequest(), email),
        equalTo(response));
  }

  @Test
  void enumerateSnapshotRequestsByDatasetId() {
    UUID datasetId = UUID.randomUUID();
    SnapshotAccessRequestResponse responseItem =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse();
    List<SnapshotAccessRequestResponse> response = List.of(responseItem);
    when(snapshotRequestDao.enumerateByDatasetId(datasetId)).thenReturn(response);

    EnumerateSnapshotAccessRequestItem expectedItem =
        new EnumerateSnapshotAccessRequestItem()
            .id(responseItem.getId())
            .status(responseItem.getStatus())
            .createdDate(responseItem.getCreatedDate())
            .name(responseItem.getSnapshotName())
            .researchPurpose(responseItem.getSnapshotResearchPurpose())
            .createdBy(responseItem.getCreatedBy());
    EnumerateSnapshotAccessRequest expected =
        new EnumerateSnapshotAccessRequest().addItemsItem(expectedItem);

    assertThat(
        "EnumerateByDatasetId returns the expected response",
        snapshotBuilderService.enumerateByDatasetId(datasetId),
        equalTo(expected));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  public void getConceptChildren(CloudPlatform cloudPlatform) {
    Dataset dataset =
        new Dataset(new DatasetSummary().cloudPlatform(cloudPlatform))
            .projectResource(new GoogleProjectResource().googleProjectId("project123"))
            .name("dataset123")
            .id(UUID.randomUUID());
    when(datasetService.retrieve(dataset.getId())).thenReturn(dataset);
    CloudPlatformWrapper cloudPlatformWrapper = CloudPlatformWrapper.of(cloudPlatform);
    var concepts = List.of(new SnapshotBuilderConcept().name("concept1").id(1));
    if (cloudPlatformWrapper.isGcp()) {
      when(bigQueryDatasetPdao.<SnapshotBuilderConcept>runQuery(any(), any(), any()))
          .thenReturn(concepts);
    } else {
      when(azureSynapsePdao.<SnapshotBuilderConcept>runQuery(any(), any())).thenReturn(concepts);
    }
    var response = snapshotBuilderService.getConceptChildren(dataset.getId(), 1, null);
    assertThat(
        "getConceptChildren returns the expected response",
        response.getResult(),
        equalTo(concepts));
  }

  @Test
  void getConceptChildrenEmpty() throws InterruptedException {
    AuthenticatedUserRequest testUser = AuthenticationFixtures.randomUserRequest();
    UUID datasetId = UUID.randomUUID();
    DatasetModel datasetModel =
        new DatasetModel().name("name");
    Schema schema = Schema.of();
    TableResult tableResult = new TableResult(schema, 1, new EmptyTableResult(schema));
    when(datasetService.retrieveDatasetModel(datasetId, testUser)).thenReturn(datasetModel);
    when(datasetService.query( anyString(), eq(datasetId)))
        .thenReturn(tableResult);
    SnapshotBuilderGetConceptsResponse expected = new SnapshotBuilderGetConceptsResponse();
    assertEquals(snapshotBuilderService.getConceptChildren(datasetId, 100, testUser), expected);
  }
}
