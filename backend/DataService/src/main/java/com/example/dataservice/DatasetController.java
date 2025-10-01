package com.example.dataservice;

import jakarta.validation.Valid;
import org.springframework.core.io.Resource;
import org.springframework.hateoas.CollectionModel;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.IanaLinkRelations;
import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.mediatype.problem.Problem;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@CrossOrigin(origins = "http://localhost:5173")
@RestController
@RequestMapping("/datasets")
public class DatasetController {

    private final DatasetRepository datasetRepository;
    private final DatasetModelAssembler datasetAssembler;
    private final DatasetService datasetService;

    public DatasetController(DatasetRepository datasetRepository,  DatasetModelAssembler datasetAssembler, DatasetService datasetService) {

        this.datasetRepository = datasetRepository;
        this.datasetAssembler = datasetAssembler;
        this.datasetService = datasetService;
    }

    @PostMapping
    public ResponseEntity<?> newDataset(
            @Valid @RequestPart("dataset") Dataset dataset,
            @RequestPart("file") MultipartFile file) {

        if (file.isEmpty()) {

            return ResponseEntity
                    .status(HttpStatus.BAD_REQUEST)
                    .header(HttpHeaders.CONTENT_TYPE, MediaTypes.HTTP_PROBLEM_DETAILS_JSON_VALUE)
                    .body(Problem.create()
                            .withTitle("Empty file")
                            .withDetail("You are not allowed to upload an empty file"));
        }

        DatasetService.FileResponse response = datasetService.uploadFile(file, dataset.getFileFormat());
        dataset.setHash(response.hash());
        dataset.setOriginalFilename(response.originalName());
        dataset.setSize(file.getSize());

        DatasetService.FileNumbers fn = datasetService.processNewDataset(dataset);
        dataset.setNumAttributes(fn.numAttributes());
        dataset.setNumEntries(fn.numEntries());

        EntityModel<Dataset> entityModel = datasetAssembler.toModel(datasetRepository.save(dataset));

        return ResponseEntity
                .created(entityModel.getRequiredLink(IanaLinkRelations.SELF).toUri())
                .body(entityModel);
    }

    @GetMapping
    public CollectionModel<EntityModel<Dataset>> all() {

        List<EntityModel<Dataset>> datasets = datasetRepository.findAll().stream()
                .map(datasetAssembler::toModel)
                .collect(Collectors.toList());

        return CollectionModel.of(datasets, linkTo(methodOn(DatasetController.class).all()).withSelfRel());
    }

    @GetMapping("/{id}")
    public EntityModel<Dataset> one(@PathVariable Long id) {

        Dataset dataset = datasetRepository.findById(id)
                .orElseThrow(() -> new DatasetNotFoundException(id));

        return datasetAssembler.toModel(dataset);
    }

    @GetMapping("/{id}/file")
    public ResponseEntity<Resource> downloadFile(@PathVariable Long id) {

        try {
            Dataset dataset =  datasetRepository.findById(id)
                    .orElseThrow(() -> new DatasetNotFoundException(id));

            Resource resource = datasetService.getFile(dataset);

            return ResponseEntity
                .ok()
                .header(
                    HttpHeaders.CONTENT_DISPOSITION,
                    "attachment; filename=\"" + dataset.getOriginalFilename() + "\""
                )
                .header(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION)
                .contentType(MediaType.parseMediaType(dataset.getFileFormat().getContentType()))
                .body(resource);

        }
        catch (IOException ex) {
            return ResponseEntity.internalServerError().build();
        }

    }

    @DeleteMapping("/{id}/delete")
    public ResponseEntity<?> delete(@PathVariable Long id) {

        datasetRepository.deleteById(id);

        return ResponseEntity.noContent().build();
    }
}
