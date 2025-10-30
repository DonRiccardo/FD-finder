package cz.cuni.mff.fdfinder.hyfdservice;

import cz.cuni.mff.fdfinder.hyfdservice.model.JobDto;
import cz.cuni.mff.fdfinder.hyfdservice.service.HyfdService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Controller of the HyFDService.
 */
@RestController
@RequestMapping("/hyfd")
public class HyfdController {

    private final HyfdService hyfdService;

    public HyfdController(HyfdService hyfdService) {

        this.hyfdService = hyfdService;
    }

    /**
     * Start a job with specified id and JobDto class.
     * @param id if of the job
     * @param job job data
     * @return HTTP OK
     */
    @PostMapping("/start/{id}")
    public ResponseEntity<?> start(@PathVariable Long id, @RequestBody JobDto job) {

        hyfdService.registerNewJob(job);

        return ResponseEntity.ok().build();
    }

    /**
     * Cancel a job with specified id.
     * @param id if of the job
     * @return HTTP OK
     */
    @PostMapping("/cancel/{id}")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        hyfdService.cancelJob(id);

        return ResponseEntity.ok().build();
    }

}
