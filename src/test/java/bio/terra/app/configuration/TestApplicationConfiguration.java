package bio.terra.app.configuration;

import bio.terra.service.dataset.AssetDao;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetRelationshipDao;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.dataset.DatasetTableDao;
import bio.terra.service.dataset.StorageResourceDao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.filedata.azure.tables.TableDirectoryDao;
import bio.terra.service.filedata.azure.tables.TableFileDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryDao;
import bio.terra.service.load.LoadDao;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.azure.AzureResourceDao;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.search.SnapshotSearchMetadataDao;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotMapTableDao;
import bio.terra.service.snapshot.SnapshotRelationshipDao;
import bio.terra.service.snapshot.SnapshotStorageAccountDao;
import bio.terra.service.snapshot.SnapshotTableDao;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@Configuration
@ConditionalOnProperty(prefix = "datarepo", name = "testWithDatabase", havingValue = "false")
public class TestApplicationConfiguration extends ApplicationConfiguration {

  @MockBean(name = "jdbcTemplate")
  public NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  @MockBean public StairwayJdbcConfiguration stairwayJdbcConfiguration;
  @MockBean public DataRepoJdbcConfiguration dataRepoJdbcConfiguration;
  @MockBean public AssetDao assetDao;
  @MockBean public AzureResourceDao azureResourceDao;
  @MockBean public DatasetBucketDao datasetBucketDao;
  @MockBean public DatasetDao datasetDao;
  @MockBean public DatasetRelationshipDao datasetRelationshipDao;
  @MockBean public DatasetStorageAccountDao datasetStorageAccountDao;
  @MockBean public DatasetTableDao datasetTableDao;
  @MockBean public FireStoreDao fireStoreDao;
  @MockBean public FireStoreDependencyDao fireStoreDependencyDao;
  @MockBean public FireStoreDirectoryDao fireStoreDirectoryDao;
  @MockBean public GoogleResourceDao googleResourceDao;
  @MockBean public LoadDao loadDao;
  @MockBean public ProfileDao profileDao;
  @MockBean public SnapshotDao snapshotDao;
  @MockBean public SnapshotMapTableDao snapshotMapTableDao;
  @MockBean public SnapshotRelationshipDao snapshotRelationshipDao;
  @MockBean public SnapshotSearchMetadataDao snapshotSearchMetadataDao;
  @MockBean public SnapshotStorageAccountDao snapshotStorageAccountDao;
  @MockBean public SnapshotTableDao snapshotTableDao;
  @MockBean public StorageResourceDao storageResourceDao;
  @MockBean public TableDao tableDao;
  @MockBean public TableDependencyDao tableDependencyDao;
  @MockBean public TableDirectoryDao tableDirectoryDao;
  @MockBean public TableFileDao tableFileDao;

  @Override
  @Bean
  public SmartInitializingSingleton postSetupInitialization(ApplicationContext applicationContext) {
    return () -> {};
  }
}
