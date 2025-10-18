package cz.cuni.mff.fdfinder.fdepservice;

import cz.cuni.mff.fdfinder.fdepservice.model.JobDto;
import cz.cuni.mff.fdfinder.fdepservice.service.FdepService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/fdep")
public class FdepController {

    private final FdepService fdepService;

    public FdepController(FdepService fdepService) {

        this.fdepService = fdepService;
    }

    @PostMapping("/start/{id}")
    public ResponseEntity<?> start(@PathVariable Long id, @RequestBody JobDto job) {

        fdepService.registerNewJob(job);

        return ResponseEntity.ok().build();
    }

    @PostMapping("/cancel/{id}")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        // TODO cancel job
        fdepService.cancelJob(id);

        return ResponseEntity.ok().build();
    }

}
