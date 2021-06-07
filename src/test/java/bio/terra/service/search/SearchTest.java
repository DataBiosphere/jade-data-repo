package bio.terra.service.search;

import bio.terra.common.category.Unit;
import bio.terra.model.SearchIndexRequest;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@Category(Unit.class)
public class SearchTest {
    @Mock
    private BigQueryPdao bigQueryPdao;

    @Mock
    private RestHighLevelClient client;

    private SearchService service;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        service = new SearchService(bigQueryPdao, client);
    }

    @Test
    public void indexSnapshotTest() throws Exception {
        Snapshot snapshot = new Snapshot();
        SearchIndexRequest searchIndexRequest = new SearchIndexRequest();

        service.indexSnapshot(snapshot, searchIndexRequest);
    }
}
