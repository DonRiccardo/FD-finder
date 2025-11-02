package cz.cuni.mff.fdfinder.fastfdsservice;

import cz.cuni.mff.fdfinder.fastfdsservice.model.JobDto;
import cz.cuni.mff.fdfinder.fastfdsservice.service.FastfdsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Controller fo the FastFDsService.
 */
@RestController
@RequestMapping("/fastfds")
public class FastfdsController {

    private final FastfdsService fastfdsService;

    public FastfdsController(FastfdsService fastfdsService) {

        this.fastfdsService = fastfdsService;
    }

    /**
     * Start a job with specified id and JobDto class.
     * @param id if of the job
     * @param job job data
     * @return HTTP OK
     */
    @PostMapping("/start/{id}")
    public ResponseEntity<?> start(@PathVariable Long id, @RequestBody JobDto job) {

        fastfdsService.registerNewJob(job);

        return ResponseEntity.ok().build();
    }

    /**
     * Cancel a job with specified id.
     * @param id if of the job
     * @return HTTP OK
     */
    @PostMapping("/cancel/{id}")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        fastfdsService.cancelJob(id);

        return ResponseEntity.ok().build();
    }

}
