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
    public ResponseEntity<?> start(@PathVariable Long id) {

        fdepServiceService.registerNewJob(id);

        return ResponseEntity.ok().build();
    }

}
