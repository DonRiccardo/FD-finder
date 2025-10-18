package cz.cuni.mff.fdfinder.demoalgservice;

import cz.cuni.mff.fdfinder.demoalgservice.model.JobDto;
import cz.cuni.mff.fdfinder.demoalgservice.service.DemoAlgService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/demoalg") // TODO change reequest mapping
public class DemoAlgController {

    private final DemoAlgService demoAlgService;

    public DemoAlgController(DemoAlgService demoAlgService) {

        this.demoAlgService = demoAlgService;
    }

    @PostMapping("/start/{id}")
    public ResponseEntity<?> start(@PathVariable Long id, @RequestBody JobDto job) {

        // TODO - edit runJob() in DemoAlgService class
        demoAlgService.registerNewJob(job);

        return ResponseEntity.ok().build();
    }

    @PostMapping("/cancel/{id}")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        // TODO cancel job
        demoAlgService.cancelJob(id);

        return ResponseEntity.ok().build();
    }

}
