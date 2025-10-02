import React, { use, useEffect, useState } from "react";
import {
  Stack,
  Input,
  TextField,
  Button,
  Checkbox,
  Select,
  MenuItem,
  Box,
  IconButton,
  Tooltip,
  InputAdornment,
  FormControl, FormControlLabel, FormLabel,
  InputLabel,
  OutlinedInput,
  CircularProgress, Table, TableBody, TableCell, TableHead, TableRow
} from "@mui/material";
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemText from '@mui/material/ListItemText';
import Divider from '@mui/material/Divider';
import DownloadIcon from '@mui/icons-material/Download';

import { Navigate } from "react-router-dom";

export default function JobsResults() {

    const [availableJobs, setAvailableJobs] = useState([]);
    const [loading, setLoading] = useState(true);

    const [selectedJob, setSelectedJob] = useState(null);
    const [dataset, setDataset] = useState(null);
    const [jobStats, setJobStats] = useState(null);
    const [foundFDs, setFoundFDs] = useState([]);
    const [fetchingResults, setFetchingResults] = useState(false);

    useEffect(() => {

        async function fetchJobs() {
            try {
                const response = await fetch("http://localhost:8082/jobs");
                const jobsData = await response.json();
                let jobs = jobsData._embedded?.jobList || [];
                jobs = jobs.filter((job) => job.status === "DONE" && job._links?.results);
                setAvailableJobs(jobs);
            }
            catch (error) {
                console.error("Error fetching jobs:", error);
            }
            finally {
                setLoading(false);
            }
        };
        
        fetchJobs();

    }, []);

    useEffect(() => {
        setFetchingResults(true);
        resetForm();

        try{
            if (selectedJob) {
                fetch("http://localhost:8082/jobs/" + selectedJob.id + "/results")
                .then((response) => response.json())
                .then((data) => {
                    setJobStats(data);
                })
                .catch((error) => {
                    console.error("Error fetching job results:", error);
                }
                );

                fetch("http://localhost:8081/datasets/" + selectedJob.dataset)
                .then((response) => response.json())
                .then((data) => {
                    setDataset(data);
                })
                .catch((error) => {
                    console.error("Error fetching dataset:", error);
                }
                );

                fetch("http://localhost:8082/jobs/" + selectedJob.id + "/results/fds")
                .then((response) => response.text())
                .then((data) => {
                    const fdsArray = data.trim().split("\n").filter((line) => line.trim() !== "");
                    return fdsArray.map((fd) => {
                        const [lhsRaw, rhsRaw] = fd.split("->");
                        const lhs = lhsRaw
                            .replace(/\[|\]/g, "")
                            .split(",")
                            .map((att) => att.trim().replace(selectedJob.datasetName + ".", ""))
                            .join(",");

                        const rhs = rhsRaw.trim().replace(selectedJob.datasetName + ".", "");

                        return { lhs: lhs, rhs: rhs };
                    });
                })
                .then((fdsArray) => {
                    setFoundFDs(fdsArray);
                })
                .catch((error) => {
                    console.error("Error fetching FDs:", error);
                }
                );

            }
            else {
                resetForm();
            }
        }
        catch (error) {
            console.error("Error fetching job results or dataset:", error);
        }
        finally {
            setFetchingResults(false);
        }

    }, [selectedJob]);

    const resetForm = () => {
        
        setDataset(null);
        setJobStats(null);
        setFoundFDs([]);
    };

    const handleDownload = () => {
        if (selectedJob && selectedJob._links?.results
            && jobStats && jobStats._links?.resultsFds
        ) {
            fetch(jobStats._links.resultsFds.href, {
                method: 'GET',
            })
            .then((response) => {
                if (response.ok) {
                    return response;
                }
                else {
                    throw new Error("Network response was not ok");
                }   
            })
            .then((response) => {
                
                const disposition = response.headers.get("Content-Disposition");
                let filename = "job-" + selectedJob.id + "-foundFDs.txt";

                if (disposition && disposition.indexOf("filename") !== -1) {
                    filename = disposition.split("filename=")[1].replace(/"/g, "").trim();

                const url = window.URL.createObjectURL(new Blob([response.blob]));
                const link = document.createElement('a');

                link.href = url;
                link.download = filename; // použije meno zo servera
                document.body.appendChild(link);
                link.click();
                link.remove();

                window.URL.revokeObjectURL(url);
                }
            })
            .catch((error) => {
                console.error("Error downloading found FDs:", error);
                alert(`Error downloading found FDs of a job"${selectedJob.id}".`);
            });
        }       
    }


    const styleList = {
        py: 0,
        width: '100%',
        maxWidth: 580,
        borderRadius: 2,
        border: '1px solid',
        borderColor: 'divider',
        backgroundColor: 'background.paper',
    };


    return (
        <>
            <Box
            sx={{
                width: "80%",
                margin: "auto auto 15px auto",
                padding: "1%",
                border: "1px solid black",
                borderRadius: "16px",
                display: "flex", 
                flexDirection: "row",
                minHeight: "400px",
            }}
            >
            {/* Left side: info */}
            <Box sx={{ flex: 1, pr: 2 }}>                    
            <Stack
                component="form"
                onSubmit={(event) => handleSubmit(event)}
                onReset={() => resetForm()}
                spacing={3}
                justifyContent="center"
                alignItems="center"
            >
            {/* Select job */}
            <FormControl fullWidth required disabled={loading}>
                <InputLabel id="job-label">Job</InputLabel>
                <Select
                    labelId="job-label"
                    value={selectedJob}
                    label="Job"
                    onChange={(e) => setSelectedJob(e.target.value)}                        
                >
                    {availableJobs.map((job) => ( 
                        <MenuItem key={job.id} value={job}><b>ID:</b> {job.id}  <b>ALG:</b> {job.algorithm}  <b>DATASET:</b> {job.datasetName}</MenuItem>
                    ))}
                </Select>
            </FormControl>

            {/* Details */}
            {fetchingResults ? 
                <div>Loading results... <CircularProgress /> </div> 
                : 
                <>
                    {jobStats && dataset ? 
                        <>
                         <h3>Job Details</h3>                            
                            
                         <List sx={styleList}>
                            <ListItem>
                                <ListItemText primary="Job ID:" 
                                secondary={selectedJob.id}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                                <ListItemText primary="Algorithm:" 
                                secondary={selectedJob.algorithm}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                                <ListItemText primary="Dataset:" 
                                secondary={`${selectedJob.datasetName} (Attributes: ${dataset.numAttributes}, Entries: ${dataset.numEntries})`}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                               <ListItemText primary="Status:" 
                                secondary={selectedJob.status}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                               <ListItemText primary="LIMIT entries:" 
                                secondary={selectedJob.limitEntries}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                               <ListItemText primary="SKIP entries:" 
                                secondary={selectedJob.skipEntries}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                               <ListItemText primary="MAX LHS:" 
                                secondary={selectedJob.maxLHS}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                               <ListItemText primary="# Found FDS:" 
                                secondary={jobStats.numFoundFd}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                               <ListItemText primary="Duration:" 
                                secondary={jobStats.duration + " ms"}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                        </List></>
                        :
                        <p>Please select a job to see details.</p>
                    }
                </>}
        
            </Stack>
            </Box>

            {/* Right side: fds */}
            <Box sx={{ flex: 1, pl: 2, borderLeft: "1px solid grey", overflowX:"auto" }}>
                
                { foundFDs.length > 0 ?
                <>
                    <Stack spacing={5} direction="row">
                        <h3>Found FDs</h3>
                        <Tooltip title="Download Dataset">
                        <IconButton 
                            aria-label="download"
                            variant="outlined" 
                            size="small"
                            sx={{ mr: 1 }}
                            onClick={() => handleDownload()}
                        >
                            <DownloadIcon />
                        </IconButton>
                        </Tooltip>
                    </Stack>
                    
                    <FDTable fds={foundFDs} />
                </>                
                :
                <></>
                }
                
            </Box>

        </Box>
        </>
    );


}

function FDTable({ fds }) {

  return (
    <Table>
      <TableHead>
        <TableRow>
          <TableCell>LHS</TableCell>
          <TableCell>RHS</TableCell>
        </TableRow>
      </TableHead>
      <TableBody>
        {fds.map((fd, idx) => (
          <TableRow key={idx}>
            <TableCell>{fd.lhs}</TableCell>
            <TableCell>{fd.rhs}</TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}