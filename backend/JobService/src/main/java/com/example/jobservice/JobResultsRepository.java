package com.example.jobservice;

import com.example.jobservice.model.JobResult;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JobResultsRepository extends JpaRepository<JobResult,Long> {
}
