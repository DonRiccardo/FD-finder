package cz.cuni.mff.fdfinder.taneservice;

import cz.cuni.mff.fdfinder.taneservice.model.JobDto;
import cz.cuni.mff.fdfinder.taneservice.service.TaneService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/tane") // TODO change reequest mapping
public class TaneController {

    private final TaneService taneService;

    public TaneController(TaneService taneService) {

        this.taneService = taneService;
    }

    @PostMapping("/start/{id}")
    public ResponseEntity<?> start(@PathVariable Long id, @RequestBody JobDto job) {

        // TODO - edit runJob() in DemoAlgService class
        taneService.registerNewJob(job);

        return ResponseEntity.ok().build();
    }

    @PostMapping("/cancel/{id}")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        // TODO cancel job
        taneService.cancelJob(id);

        return ResponseEntity.ok().build();
    }

}
