package com.example.jobservice.model;

import com.example.jobservice.JobController;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.RepresentationModelAssembler;
import org.springframework.stereotype.Component;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

public enum JobStatus {
    CREATED,    // user can START job
    WAITING,    // user can CANCEL job
    RUNNING,    // user can CANCEL jon
    CANCELLED,  // user can reSTART job
    FAILED,     // user can do NOTHING
    DONE        // user can do nothing?
    ;

}
