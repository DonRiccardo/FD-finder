package cz.cuni.mff.fdfinder.fdepservice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/fdep")
public class FdepServiceController {

    private final FdepServiceService fdepServiceService;

    public FdepServiceController(FdepServiceService fdepServiceService) {

        this.fdepServiceService = fdepServiceService;
    }

    @PostMapping("/start/{id}")
    public ResponseEntity<?> start(@PathVariable Long id, @RequestBody JobDto job) {

        fdepServiceService.registerNewJob(job);

        return ResponseEntity.ok().build();
    }

    @PostMapping("/cancel/{id}")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        // TODO cancel job

        return ResponseEntity.ok().build();
    }

}
