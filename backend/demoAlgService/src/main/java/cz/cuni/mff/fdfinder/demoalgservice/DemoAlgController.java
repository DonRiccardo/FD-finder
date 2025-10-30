package cz.cuni.mff.fdfinder.demoalgservice;

import cz.cuni.mff.fdfinder.demoalgservice.model.JobDto;
import cz.cuni.mff.fdfinder.demoalgservice.service.DemoAlgService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Controller for DemoAlgService. CrossOrigin exception for frontend.
 */
@RestController
@RequestMapping("/demoalg") // TODO change reequest mapping
public class DemoAlgController {

    private final DemoAlgService demoAlgService;

    public DemoAlgController(DemoAlgService demoAlgService) {

        this.demoAlgService = demoAlgService;
    }

    /**
     * Start a job with specified id and JobDto class.
     * @param id if of the job
     * @param job job data
     * @return HTTP OK
     */
    @PostMapping("/start/{id}")
    public ResponseEntity<?> start(@PathVariable Long id, @RequestBody JobDto job) {

        // TODO - edit runJob() in DemoAlgService class
        demoAlgService.registerNewJob(job);

        return ResponseEntity.ok().build();
    }

    /**
     * Cancel a job with specified id.
     * @param id if of the job
     * @return HTTP OK
     */
    @PostMapping("/cancel/{id}")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        // TODO cancel job
        demoAlgService.cancelJob(id);

        return ResponseEntity.ok().build();
    }

}
