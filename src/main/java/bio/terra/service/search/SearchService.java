package bio.terra.service.search;

import bio.terra.model.SearchIndexModel;
import bio.terra.model.SearchIndexRequest;
import bio.terra.service.snapshot.Snapshot;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SearchService {

    @Autowired
    public SearchService() {
        // empty constructor
    }

    public SearchIndexModel indexSnapshot(Snapshot snapshot, SearchIndexRequest searchIndexRequest) {
        String sqlQuery = searchIndexRequest.getSqlQuery();
        return new SearchIndexModel();
    }
}
