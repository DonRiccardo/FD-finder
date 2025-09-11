package com.example.fdepservice;

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

        fdepServiceService.startJob(id);

        return ResponseEntity.ok().build();
    }

}
