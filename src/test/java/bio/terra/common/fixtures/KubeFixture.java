package bio.terra.common.fixtures;

import bio.terra.service.kubernetes.KubeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public final class KubeFixture {
    @Autowired
    private KubeFixture() {
    }

    @Autowired private KubeService kubeService;

    public List<String> listAllPods() {
        kubeService.startPodListener();
        return kubeService.getApiPodList();
    }
}
