package cz.cuni.mff.fdfinder.depminerservice;

import cz.cuni.mff.fdfinder.depminerservice.model.JobDto;
import cz.cuni.mff.fdfinder.depminerservice.service.DepMinerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/depminer")
public class DepMinerController {

    private final DepMinerService depMinerService;

    public DepMinerController(DepMinerService depMinerService) {

        this.depMinerService = depMinerService;
    }

    @PostMapping("/start/{id}")
    public ResponseEntity<?> start(@PathVariable Long id, @RequestBody JobDto job) {

        // TODO - edit runJob() in DemoAlgService class
        depMinerService.registerNewJob(job);

        return ResponseEntity.ok().build();
    }

    @PostMapping("/cancel/{id}")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        // TODO cancel job
        depMinerService.cancelJob(id);

        return ResponseEntity.ok().build();
    }

}
