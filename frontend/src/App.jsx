import { useState } from 'react'
import * as React from "react";
import { Routes, Route, useParams, Link } from "react-router-dom";
import { Button, Stack, Box, CssBaseline } from "@mui/material";
import { BaseUrlContext } from "./utils/base-url-context";
import Navbar from './utils/navbar';
import DatasetsAddPage from './pages/datasets-add';
import DatasetsAll from './pages/datasets-all';
import JobsAll from './pages/jobs-all';
import JobsCreate from './pages/jobs-add';
import JobsResults from './pages/jobs-results';

function App() {
  

  return (
    <>
        <CssBaseline enableColorScheme />
        <Navbar />
        <Box sx={{ textAlign: "center" }}>
          <Routes>
            <Route path="/" element={<HomePage />} />
            <Route path="/datasets" element={<Datasets />} />
            <Route path="/datasets/upload" element={<DatasetsAdd />} />
            <Route path="/jobs" element={<Jobs />} />
            <Route path="/jobs/create" element={<JobsAdd />} />
            <Route path="/jobs/create/:datasetId" element={<JobDataset />} />
            <Route path="/jobs/results" element={<JobResult />} />
            <Route path="/jobs/results/:jobId" element={<JobResultId />} />
          </Routes>
        </Box>
    </>
  )
}

function HomePage() {
  return (
    <Box
      sx={{
        display: "flex",
        width: "100%",
        height: "100%",
        position: "absolute",
        justifyContent: "center",
        alignItems: "center",
      }}
    >
      <div>
        <main>
          <h1>Welcome to FD-Finder APP!</h1>
          <p>Upload your dataset to get started.</p>
        </main>
        <nav>
          <Stack spacing={3} justifyContent="center" alignItems="center">
            <Link to="/datasets/upload">
              <Button variant="contained">Upload Dataset</Button>
            </Link>
            <Link to="/jobs/create">
              <Button variant="contained">Create Job</Button>
            </Link>
          </Stack>
        </nav>
      </div>
    </Box>
  );

}

function DatasetsAdd() {
  return (
    <>
      <main>
        <h1>Upload new Dataset</h1>
        <DatasetsAddPage />
      </main>
    </>
  );

}

function Datasets() {
  return (
    <>
      <main>
        <h1>Datasets</h1>
        <DatasetsAll />
      </main>
    </>
  );
}

function Jobs() {
  return (
    <>
      <main>
        <h1>Jobs</h1>
        <JobsAll />
      </main>
    </>
  );
}

function JobsAdd() {
  return (
    <>
      <main>
        <h1>Create new Job</h1>
        <JobsCreate />
      </main>
    </>
  ) 
}

function JobDataset() {
  const {datasetId} = useParams();
  return (
    <>
      <main>
        <h1>Edit Job</h1>
        <JobsCreate datasetId={datasetId} />
      </main>
    </>
  )
}

function JobResult() {
  return(
    <>
      <main>
        <h1>Job Results</h1>
        <JobsResults />
      </main>
    </>
  )
}

function JobResultId() {
  const {jobId} = useParams();
  return(
    <>
      <main>
        <h1>Job Results</h1>
        <JobsResults jobId={jobId}/>
      </main>
    </>
  )
}

export default App
