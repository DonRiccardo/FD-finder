package cz.cuni.mff.fdfinder.taneservice;

import cz.cuni.mff.fdfinder.taneservice.model.JobDto;
import cz.cuni.mff.fdfinder.taneservice.service.TaneService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Controller fo the TaneService.
 */
@RestController
@RequestMapping("/tane")
public class TaneController {

    private final TaneService taneService;

    public TaneController(TaneService taneService) {

        this.taneService = taneService;
    }

    /**
     * Start a job with specified id and JobDto class.
     * @param id if of the job
     * @param job job data
     * @return HTTP OK
     */
    @PostMapping("/start/{id}")
    public ResponseEntity<?> start(@PathVariable Long id, @RequestBody JobDto job) {

        taneService.registerNewJob(job);

        return ResponseEntity.ok().build();
    }

    /**
     * Cancel a job with specified id.
     * @param id if of the job
     * @return HTTP OK
     */
    @PostMapping("/cancel/{id}")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        taneService.cancelJob(id);

        return ResponseEntity.ok().build();
    }

}
