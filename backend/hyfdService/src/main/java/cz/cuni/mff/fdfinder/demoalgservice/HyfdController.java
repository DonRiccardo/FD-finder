package cz.cuni.mff.fdfinder.demoalgservice;

import cz.cuni.mff.fdfinder.demoalgservice.model.JobDto;
import cz.cuni.mff.fdfinder.demoalgservice.service.HyfdService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/hyfd") // TODO change reequest mapping
public class HyfdController {

    private final HyfdService hyfdService;

    public HyfdController(HyfdService hyfdService) {

        this.hyfdService = hyfdService;
    }

    @PostMapping("/start/{id}")
    public ResponseEntity<?> start(@PathVariable Long id, @RequestBody JobDto job) {

        // TODO - edit runJob() in DemoAlgService class
        hyfdService.registerNewJob(job);

        return ResponseEntity.ok().build();
    }

    @PostMapping("/cancel/{id}")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        // TODO cancel job
        hyfdService.cancelJob(id);

        return ResponseEntity.ok().build();
    }

}
