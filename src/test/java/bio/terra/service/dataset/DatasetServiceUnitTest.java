package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.common.MetadataEnumeration;
import bio.terra.common.category.Unit;
import bio.terra.service.iam.IamRole;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadService;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.tabulardata.azure.StorageTableService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {DatasetService.class})
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DatasetServiceUnitTest {

  @MockBean private DatasetDao datasetDao;

  @Autowired private DatasetService datasetService;

  @MockBean private JobService jobService;
  @MockBean private LoadService loadService;
  @MockBean private ProfileDao profileDao;
  @MockBean private StorageTableService storageTableService;
  @MockBean private BigQueryPdao bigQueryPdao;
  @MockBean private MetadataDataAccessUtils metadataDataAccessUtils;

  @Test
  public void enumerate() {
    UUID uuid = UUID.randomUUID();
    IamRole role = IamRole.DISCOVERER;
    Map<UUID, Set<IamRole>> resourcesAndRoles = Map.of(uuid, Set.of(role));
    MetadataEnumeration<DatasetSummary> metadataEnumeration = new MetadataEnumeration<>();
    DatasetSummary summary =
        new DatasetSummary().id(uuid).createdDate(Instant.now()).storage(List.of());
    metadataEnumeration.items(List.of(summary));
    when(datasetDao.enumerate(
            anyInt(), anyInt(), any(), any(), any(), any(), eq(resourcesAndRoles.keySet())))
        .thenReturn(metadataEnumeration);
    var datasets = datasetService.enumerate(0, 10, null, null, null, null, resourcesAndRoles);
    assertThat(datasets.getItems().get(0).getId(), equalTo(uuid));
    assertThat(datasets.getRoleMap(), hasEntry(uuid.toString(), List.of(role.toString())));
  }
}
