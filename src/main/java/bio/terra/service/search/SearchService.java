package bio.terra.service.search;

import bio.terra.model.SearchIndexModel;
import bio.terra.model.SearchIndexRequest;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class SearchService {

    private final BigQueryPdao bigQueryPdao;

    @Autowired
    public SearchService(BigQueryPdao bigQueryPdao) {
        this.bigQueryPdao = bigQueryPdao;
    }

    public SearchIndexModel indexSnapshot(Snapshot snapshot, SearchIndexRequest searchIndexRequest)
        throws InterruptedException {
        List<Map<String, Object>> values = bigQueryPdao.getSnapshotTableData(snapshot, searchIndexRequest.getSql());
        return new SearchIndexModel();
    }
}
