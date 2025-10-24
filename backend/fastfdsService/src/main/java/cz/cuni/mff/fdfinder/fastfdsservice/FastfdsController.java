package cz.cuni.mff.fdfinder.fastfdsservice;

import cz.cuni.mff.fdfinder.fastfdsservice.model.JobDto;
import cz.cuni.mff.fdfinder.fastfdsservice.service.FastfdsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/fastfds") // TODO change reequest mapping
public class FastfdsController {

    private final FastfdsService fastfdsService;

    public FastfdsController(FastfdsService fastfdsService) {

        this.fastfdsService = fastfdsService;
    }

    @PostMapping("/start/{id}")
    public ResponseEntity<?> start(@PathVariable Long id, @RequestBody JobDto job) {

        // TODO - edit runJob() in DemoAlgService class
        fastfdsService.registerNewJob(job);

        return ResponseEntity.ok().build();
    }

    @PostMapping("/cancel/{id}")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        // TODO cancel job
        fastfdsService.cancelJob(id);

        return ResponseEntity.ok().build();
    }

}
