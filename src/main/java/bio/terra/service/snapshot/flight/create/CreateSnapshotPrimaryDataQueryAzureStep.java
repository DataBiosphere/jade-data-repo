package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.grammar.Query;
import bio.terra.grammar.azure.SynapseVisitor;
import bio.terra.model.DatasetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.stairway.FlightContext;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CreateSnapshotPrimaryDataQueryAzureStep extends CreateSnapshotParquetFilesAzureStep {

  private final AzureSynapsePdao azureSynapsePdao;
  private final SnapshotService snapshotService;
  private final SnapshotDao snapshotDao;
  private final SnapshotRequestModel snapshotReq;
  private final DatasetService datasetService;
  private final AuthenticatedUserRequest userRequest;
  private final CreateSnapshotPrimaryDataQueryUtils createSnapshotPrimaryDataQueryUtils;

  public CreateSnapshotPrimaryDataQueryAzureStep(
      AzureSynapsePdao azureSynapsePdao,
      SnapshotDao snapshotDao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq,
      DatasetService datasetService,
      AuthenticatedUserRequest userRequest,
      CreateSnapshotPrimaryDataQueryUtils createSnapshotPrimaryDataQueryUtils) {
    super(azureSynapsePdao, snapshotService);
    this.azureSynapsePdao = azureSynapsePdao;
    this.snapshotService = snapshotService;
    this.snapshotReq = snapshotReq;
    this.snapshotDao = snapshotDao;
    this.datasetService = datasetService;
    this.userRequest = userRequest;
    this.createSnapshotPrimaryDataQueryUtils = createSnapshotPrimaryDataQueryUtils;
  }

  @Override
  public Map<String, Long> createSnapshotParquetFiles(
      List<SnapshotTable> tables, UUID snapshotId, FlightContext context) throws SQLException {
    SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(0);
    SnapshotRequestQueryModel queryModel = contentsModel.getQuerySpec();

    Query query = Query.parse(queryModel.getQuery());
    Dataset dataset = createSnapshotPrimaryDataQueryUtils.retrieveDatasetSpecifiedByQuery(query);
    AssetSpecification assetSpecification =
        createSnapshotPrimaryDataQueryUtils.retrieveAssetSpecification(
            dataset, queryModel.getAssetName());
    createSnapshotPrimaryDataQueryUtils.validateRootTable(query, assetSpecification);

    String sourceDatasetDataSourceName =
        IngestUtils.getSourceDatasetDataSourceName(context.getFlightId());
    String sqlQuery = translateSynapseQuery(query, dataset, sourceDatasetDataSourceName);

    return azureSynapsePdao.createSnapshotParquetFilesByQuery(
        assetSpecification,
        snapshotId,
        sourceDatasetDataSourceName,
        IngestUtils.getTargetDataSourceName(context.getFlightId()),
        sqlQuery);
  }

  private String translateSynapseQuery(
      Query query, Dataset dataset, String sourceDatasetDatasource) {
    DatasetModel datasetModel = datasetService.retrieveModel(dataset, userRequest);
    Map<String, DatasetModel> datasetMap =
        Collections.singletonMap(dataset.getName(), datasetModel);
    SynapseVisitor synapseVisitor = new SynapseVisitor(datasetMap, sourceDatasetDatasource);
    return query.translateSql(synapseVisitor);
  }
}
