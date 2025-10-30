package cz.cuni.mff.fdfinder.depminerservice;

import cz.cuni.mff.fdfinder.depminerservice.model.JobDto;
import cz.cuni.mff.fdfinder.depminerservice.service.DepMinerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Controller fo the DepMinerService.
 */
@RestController
@RequestMapping("/depminer")
public class DepMinerController {

    private final DepMinerService depMinerService;

    public DepMinerController(DepMinerService depMinerService) {

        this.depMinerService = depMinerService;
    }

    /**
     * Start a job with specified id and JobDto class.
     * @param id if of the job
     * @param job job data
     * @return HTTP OK
     */
    @PostMapping("/start/{id}")
    public ResponseEntity<?> start(@PathVariable Long id, @RequestBody JobDto job) {

        depMinerService.registerNewJob(job);

        return ResponseEntity.ok().build();
    }

    /**
     * Cancel a job with specified id.
     * @param id if of the job
     * @return HTTP OK
     */
    @PostMapping("/cancel/{id}")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        depMinerService.cancelJob(id);

        return ResponseEntity.ok().build();
    }

}
