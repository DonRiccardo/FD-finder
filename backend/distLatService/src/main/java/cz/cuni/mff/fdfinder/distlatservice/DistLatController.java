package cz.cuni.mff.fdfinder.distlatservice;

import cz.cuni.mff.fdfinder.distlatservice.model.JobDto;
import cz.cuni.mff.fdfinder.distlatservice.service.DistLatService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Controller fo the TaneService.
 */
@RestController
@RequestMapping("/distlat")
public class DistLatController {

    private final DistLatService distLatService;

    public DistLatController(DistLatService distLatService) {

        this.distLatService = distLatService;
    }

    /**
     * Start a job with specified id and JobDto class.
     * @param id if of the job
     * @param job job data
     * @return HTTP OK
     */
    @PostMapping("/start/{id}")
    public ResponseEntity<?> start(@PathVariable Long id, @RequestBody JobDto job) {

        distLatService.registerNewJob(job);

        return ResponseEntity.ok().build();
    }

    /**
     * Cancel a job with specified id.
     * @param id if of the job
     * @return HTTP OK
     */
    @PostMapping("/cancel/{id}")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        distLatService.cancelJob(id);

        return ResponseEntity.ok().build();
    }

}
