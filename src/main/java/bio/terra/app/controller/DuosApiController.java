package bio.terra.app.controller;

import bio.terra.controller.DuosApi;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.duos.DuosService;
import io.swagger.annotations.Api;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
@Api(tags = {"duos"})
public class DuosApiController implements DuosApi {

  private final DuosService duosService;

  @Autowired
  public DuosApiController(DuosService duosService) {
    this.duosService = duosService;
  }

  @Override
  public ResponseEntity<List<DuosFirecloudGroupModel>> syncAllFirecloudGroupContents() {
    // TODO: check DATAREPO admin privileges.
    // TODO: turn into asynchronous flight.
    return new ResponseEntity<>(duosService.syncAllFirecloudGroupContents(), HttpStatus.OK);
  }

  @Override
  public ResponseEntity<DuosFirecloudGroupModel> syncFirecloudGroupContents(String duosId) {
    // TODO: check DATAREPO admin privileges.
    return new ResponseEntity<>(duosService.syncFirecloudGroupContents(duosId), HttpStatus.OK);
  }
}
