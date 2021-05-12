package bio.terra.app.controller;

import bio.terra.controller.SearchApi;
import bio.terra.model.SearchIndexModel;
import bio.terra.model.SearchIndexRequest;
import bio.terra.model.SearchQueryRequest;
import bio.terra.model.SearchQueryResultModel;
import io.swagger.annotations.Api;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import javax.validation.Valid;

@Controller
@Api(tags = {"search"})
@ConditionalOnProperty(name = "features.search.api", havingValue = "enabled")
public class SearchApiController implements SearchApi {
    private Logger logger = LoggerFactory.getLogger(RegisterApiController.class);

    @Autowired
    public SearchApiController() {
        // do nothing
    }

    @Override
    public ResponseEntity<SearchIndexModel> createSearchIndex(
        @PathVariable("id") String id,
        @Valid @RequestBody SearchIndexRequest body
    ) {
        SearchIndexModel searchIndexModel = new SearchIndexModel();
        return new ResponseEntity<>(searchIndexModel, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<SearchQueryResultModel> querySearchIndices(
        @Valid @RequestBody SearchQueryRequest body,
        @Valid @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
        @Valid @RequestParam(value = "limit", required = false, defaultValue = "1000") Integer limit
    ) {
        SearchQueryResultModel searchQueryResultModel = new SearchQueryResultModel();
        return new ResponseEntity<>(searchQueryResultModel, HttpStatus.OK);
    }
}
