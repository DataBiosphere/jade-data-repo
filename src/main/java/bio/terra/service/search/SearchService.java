package bio.terra.service.search;

import bio.terra.model.SearchIndexModel;
import bio.terra.model.SearchIndexRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SearchService {

    @Autowired
    public SearchService() {
        // empty constructor
    }

    public SearchIndexModel indexSnapshot(SearchIndexRequest searchIndexRequest) {
        return new SearchIndexModel();
    }
}
