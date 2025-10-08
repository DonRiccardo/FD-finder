import React, { use, useEffect, useState } from "react";
import {
  Stack,
  Button,
  Checkbox,
  Select,
  MenuItem,
  Box, Tab, Tabs,
  IconButton,
  Tooltip,
  FormControl,
  InputLabel,
  OutlinedInput,
  CircularProgress, Table, TableBody, TableCell, TableHead, TableRow
} from "@mui/material";
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemText from '@mui/material/ListItemText';
import Divider from '@mui/material/Divider';
import DownloadIcon from '@mui/icons-material/Download';
import FindInPageIcon from '@mui/icons-material/FindInPage';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import PropTypes from 'prop-types';
import { Navigate } from "react-router-dom";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip as ChartToolTip,
  Legend,
} from 'chart.js';
import { Line } from 'react-chartjs-2';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  ChartToolTip,
  Legend
);

const styleList = {
    py: 0,
    width: '100%',
    maxWidth: 580,
    borderRadius: 2,
    border: '1px solid',
    borderColor: 'divider',
    backgroundColor: 'background.paper',
};

const roundOrZero = (n) => 
    (typeof n === 'number' && !isNaN(n)) ? Number(n.toFixed(2)) : 0;

const roundOrZeroAndMegaBytes = (n) => 
    (typeof n === 'number' && !isNaN(n)) ? Number((n / (1024 * 1024)).toFixed(2)) : 0;

const roundOrZeroAndPercentage = (n) => 
    (typeof n === 'number' && !isNaN(n)) ? Number((n * 100).toFixed(2)) : 0;

function formatJobAlgorithmStats(stats) {
    return {
        timeMin: roundOrZero(stats.timeMin),
        timeAvg: roundOrZero(stats.timeAvg),
        timeMax: roundOrZero(stats.timeMax),
        memoryMin: roundOrZeroAndMegaBytes(stats.memoryMin),
        memoryAvg: roundOrZeroAndMegaBytes(stats.memoryAvg),
        memoryMax: roundOrZeroAndMegaBytes(stats.memoryMax),
        cpuMin: roundOrZeroAndPercentage(stats.cpuMin),
        cpuAvg: roundOrZeroAndPercentage(stats.cpuAvg),
        cpuMax: roundOrZeroAndPercentage(stats.cpuMax),
        numFdsMin: roundOrZero(stats.numFdsMin),
        numFdsAvg: roundOrZero(stats.numFdsAvg),
        numFdsMax: roundOrZero(stats.numFdsMax),
    };
}

export default function JobsResults() {

    const [availableJobs, setAvailableJobs] = useState([]);
    const [loading, setLoading] = useState(true);
    const [fetchingResults, setFetchingResults] = useState(false);
    const [selectedTabValue, setSelectedTabValue] = useState(0);
    const [showFds, setShowFds] = useState(false);

    const [selectedJob, setSelectedJob] = useState(null);
    const [selectedJobAlgorithm, setSelectedJobAlgorithm] = useState("");
    const [selectedAlgorithmIterations, setSelectedAlgorithmIterations] = useState([]);
    const [dataset, setDataset] = useState(null);
    const [jobStats, setJobStats] = useState(null);
    const [graphData, setGraphData] = useState(null);
    const [selectedIterationToShowFds, setSelectedIterationToShowFds] = useState("");
    const [foundFDs, setFoundFDs] = useState([]);

    const filteredResults = selectedJob?.jobResults.filter(
        (r) => r.algorithm === selectedJobAlgorithm
    );

    

    useEffect(() => {
        setLoading(true);

        async function fetchJobs() {
            try {
                const response = await fetch("http://localhost:8082/jobs");
                const jobsData = await response.json();
                let jobs = jobsData._embedded?.jobList || [];
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

        async function fetchResultsOfJob(params) {

            try{
                if (selectedJob) {

                    const [jobStatsRes, datasetMetadata, graphData] = await Promise.all([
                        fetch("http://localhost:8082/jobs/" + selectedJob.id + "/results")
                        .then(res => res.json())
                        .then(data => {
                            const result = {};
                            for (const [key, value] of Object.entries(data)) {
                                result[key] = formatJobAlgorithmStats(value);
                            }
                            return result;
                        } ),
                        fetch("http://localhost:8081/datasets/" + selectedJob.dataset).then(res => res.json()),                        
                        fetch("http://localhost:8082/jobs/" + selectedJob.id + "/results/graphdata").then(res => res.json())
                    ]);

                    setJobStats(jobStatsRes);
                    setDataset(datasetMetadata);
                    setGraphData(graphData);
                    
                    
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
        }
           
        
        fetchResultsOfJob() ;
    }, [selectedJob]);

    const handleShowFDs = async (jobResultId) => {
        try {
            const response = await fetch("http://localhost:8082/jobs/results/" + jobResultId + "/fds");
            const fdsRes = await response.text();

            const fdsArray = fdsRes.trim().split("\n").filter(line => line.trim() !== "").map(fd => {
                const [lhsRaw, rhsRaw] = fd.split("->");
                const lhs = lhsRaw
                    .replace(/\[|\]/g, "")
                    .split(",")
                    .map(att => att.trim().replace(selectedJob.datasetName + ".", ""))
                    .join(",");
                const rhs = rhsRaw.trim().replace(selectedJob.datasetName + ".", "");
                return { lhs, rhs };
            });
            setFoundFDs(fdsArray);
            setShowFds(true);
            setSelectedIterationToShowFds(jobResultId);
        }
        catch (error) {
            console.error("Error fetching fds:", error);
        }
        finally {
            setLoading(false);
        }
    };

    const resetForm = () => {
        
        setDataset(null);
        setJobStats(null);
        setFoundFDs([]);
        setSelectedJobAlgorithm("");
        setGraphData(null);
        setShowFds(false);
        setSelectedIterationToShowFds("");
    };

    const handleBack = () => {
        setShowFds(false);
        setSelectedIterationToShowFds("");
    }

    const handleDownload = (iterationId) => {
        if (!iterationId && !Number.isInteger(iterationId)){
            alert("Invalid iteration ID.");
            return;
        }
        {
            fetch("http://localhost:8082/jobs/results/" + iterationId + "/fds", {
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
                let filename = "job-" + selectedJob.id + "-" + selectedJobAlgorithm + "-runid-" + selectedIterationToShowFds + "-foundFDs.txt";

                if (disposition && disposition.indexOf("filename") !== -1) {
                    filename = disposition.split("filename=")[1].replace(/"/g, "").trim();

                const url = window.URL.createObjectURL(new Blob([response.blob]));
                const link = document.createElement('a');

                link.href = url;
                link.download = filename;
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
                        <MenuItem key={job.id} value={job}>{job.jobName}</MenuItem>
                    ))}
                </Select>
            </FormControl>

            {/* Details */}
            {loading || fetchingResults? 
                <div> <CircularProgress /> Loading data... </div> 
                : 
                <>
                    {jobStats && dataset ? 
                        <>
                        <Stack spacing={5} direction="row" justifyContent="space-between" width={"100%"}>
                            <h3 style={{marginLeft: 5}}>{selectedJob.jobName}</h3> 
                            <span
                                style={{ fontWeight: "bold", textAlign: "right",
                                    backgroundColor: selectedJob.status === "DONE" ? "green" : 
                                           selectedJob.status === "RUNNING" ? "blue" :
                                           selectedJob.status === "CANCELED" ? "orange" :
                                           selectedJob.status === "FAILED" ? "red" :
                                           "black",
                                    color: "white", padding: "5px", borderRadius: "8px"
                                 }}
                            >
                                {selectedJob.status}
                            </span>
                        </Stack>
                                                    
                            
                         <List sx={{ ...styleList}}>
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
                                <ListItemText primary="Job Name:" 
                                secondary={selectedJob.jobName}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                                <ListItemText primary="Job Description:" 
                                secondary={selectedJob.jobDescription || "No description"}
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
                               <ListItemText primary="Repeat:" 
                                secondary={selectedJob.repeat}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                               <ListItemText primary="LIMIT entries:" 
                                secondary={selectedJob.limitEntries > 0 ? selectedJob.limitEntries : "undefined"}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                               <ListItemText primary="SKIP entries:" 
                                secondary={selectedJob.skipEntries > 0 ? selectedJob.skipEntries : "undefined"}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                               <ListItemText primary="MAX LHS:" 
                                secondary={selectedJob.maxLHS >= 0 ? selectedJob.maxLHS : "undefined"}
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

            {/* Right side: stats, fds, graphs */}
            <Box sx={{ width: "100%", flex: 1, pl: 2, borderLeft: "1px solid grey", overflowX:"auto", overflowY:"auto" }}>
                
                { selectedJob != null && selectedJob.status === "DONE" ?
                <>
                    <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
                        <Tabs value={selectedTabValue} onChange={(e, newValue) => setSelectedTabValue(newValue)} aria-label="basic tabs example">
                        <Tab label="Statistics" id= "simple-tab-0" />
                        <Tab label="FDs" id= "simple-tab-1" />
                        <Tab label="Graphs" id= "simple-tab-2" />
                        </Tabs>
                        {selectedTabValue !== 2 ?
                            <Stack  marginTop={2}>
                            <FormControl fullWidth>
                            <InputLabel id="algorithm-label">Algorithm</InputLabel>
                            <Select
                                labelId="algorithm-label"
                                value={selectedJobAlgorithm}
                                label="Algorithm"
                                onChange={(e) => setSelectedJobAlgorithm(e.target.value)}                   
                            >
                                {selectedJob.algorithm.map((alg) => ( 
                                    <MenuItem key={alg} value={alg}>{alg}</MenuItem>
                                ))}
                            </Select>
                            </FormControl>
                            </Stack>
                        : <></>}
                        
                    </Box>
                    <CustomTabPanel value={selectedTabValue} index={0}>                        
                        { jobStats && selectedJobAlgorithm ? 
                            <JobAlgorithmStats stats={jobStats[selectedJobAlgorithm]} />
                            :
                            <p>Please select an algorithm to see statistics.</p>
                        }
                    </CustomTabPanel>
                    <CustomTabPanel value={selectedTabValue} index={1}>
                        {!showFds ?                            
                        <>
                        {selectedJobAlgorithm ? 
                            <Table>
                                <TableHead>
                                    <TableRow>
                                    <TableCell>ID</TableCell>
                                    <TableCell># iteration</TableCell>
                                    <TableCell># FDs</TableCell>
                                    <TableCell>Duration (ms)</TableCell>
                                    <TableCell align="right"></TableCell>
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {filteredResults?.map((res) => (
                                    <TableRow key={res.id}>
                                        <TableCell>{res.id}</TableCell>
                                        <TableCell>{res.iteration}</TableCell>
                                        <TableCell>{res.numFoundFd}</TableCell>
                                        <TableCell>{res.duration}</TableCell>
                                        <TableCell align="right">
                                        <Stack direction="row" spacing={1} justifyContent="flex-end">
                                            <Tooltip title="Show found FDs">
                                            <IconButton 
                                                aria-label="showfds"
                                                variant="outlined" 
                                                size="small"
                                                sx={{ mr: 1 }}
                                                onClick={() => handleShowFDs(res.id)}
                                            >
                                                <FindInPageIcon />
                                            </IconButton>
                                            </Tooltip>
                                            <Tooltip title="Download found FDs">
                                            <IconButton 
                                                aria-label="download"
                                                variant="outlined" 
                                                size="small"
                                                sx={{ mr: 1 }}
                                                onClick={() => handleDownload(res.id)}
                                            >
                                                <DownloadIcon />
                                            </IconButton>
                                            </Tooltip>
                                        </Stack>
                                        </TableCell>
                                    </TableRow>
                                    ))}
                                </TableBody>
                            </Table>    
                        : <p>Please select an algorithm to see iterations/FDs.</p>
                        }                            
                        </>
                        :
                        <>
                            <Stack spacing={5} direction="row">
                                <Tooltip title="Back">
                                <IconButton 
                                    aria-label="back"
                                    variant="outlined" 
                                    size="small"
                                    sx={{ mr: 1 }}
                                    onClick={() => handleBack()}
                                >
                                    <ArrowBackIcon />
                                </IconButton>
                                </Tooltip>
                                <Tooltip title="Download found FDs">
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
                        </>}

                        
                    </CustomTabPanel>
                    <CustomTabPanel value={selectedTabValue} index={2}>
                        { jobStats && selectedJob ? 
                            <GeneratteCpuMemoryGraphs data={graphData} />
                            :
                            <p>Nothing to show.</p>  
                    }
                    </CustomTabPanel>
                    
                </>                
                :
                <p>Nothing to show</p>
                }
                
            </Box>

        </Box>
        </>
    );


}


function CustomTabPanel(props) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
    </div>
  );
}

CustomTabPanel.propTypes = {
  children: PropTypes.node,
  index: PropTypes.number.isRequired,
  value: PropTypes.number.isRequired,
};


function JobAlgorithmStats({ stats }) {

    return (
        <List sx={{ ...styleList, marginTop: 2 }}>
            <ListItem>
                <ListItemText primary="NAME" 
                secondary={"MIN / AVG / MAX value"}
                slotProps={{
                    secondary: { sx: {  textAlign: "right" } },
                }}
                />
            </ListItem>
            <Divider variant="middle" component="li" />
            <ListItem>
                <ListItemText primary="Duration (ms):" 
                secondary={stats.timeMin + " / " + stats.timeAvg + " / " + stats.timeMax}
                slotProps={{
                    secondary: { sx: {  textAlign: "right" } },
                }}
                />
            </ListItem>
            <Divider variant="middle" component="li" />
            <ListItem>
                <ListItemText primary="Memory usage (MB):" 
                secondary={stats.memoryMin + " / " + stats.memoryAvg + " / " + stats.memoryMax}
                slotProps={{
                    secondary: { sx: {  textAlign: "right" } },
                }}
                />
            </ListItem>
            <Divider variant="middle" component="li" />
            <ListItem>
                <ListItemText primary="CPU usage (%):" 
                secondary={stats.cpuMin + " / " + stats.cpuAvg + " / " + stats.cpuMax}
                slotProps={{
                    secondary: { sx: {  textAlign: "right" } },
                }}
                />
            </ListItem>
            <Divider variant="middle" component="li" />
            <ListItem>
                <ListItemText primary="# found FDs:" 
                secondary={stats.numFdsMin + " / " + stats.numFdsAvg + " / " + stats.numFdsMax}
                slotProps={{
                    secondary: { sx: {  textAlign: "right" } },
                }}
                />
            </ListItem>
            
        </List>
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

function GeneratteCpuMemoryGraphs({ data }) {

    const xLabels = [];
    const graphDataCpu = {
        labels: xLabels,
        datasets: []
    }
    const graphDataMemory = {
        labels: xLabels,
        datasets: []
    }

    for (const [key, value] of Object.entries(data)) {

        const cpuData = [];
        const memoryData = [];

        for (let i = 0; i < value.length; i++) {
            const entry = value[i];
            xLabels.includes(entry.timestamp) ? null : xLabels.push(entry.timestamp);
            cpuData.push(entry.cpuLoad);
            memoryData.push(entry.usedMemory);
        }


        graphDataCpu.datasets.push(
            {
                label: key,
                data: cpuData,
                borderColor: "red"
            }
        )
    

        graphDataMemory.datasets.push(
            {
                label: key,
                data: memoryData
            }
        )

    }


    const optionsCpu = {
        responsive: true,
        plugins: {
            legend: {
                position: "top",
            },
            title: {
                display: true,
                text: "CPU usage over time (%)"
            }
        }
        
    };

    const optionsMemory = {
        responsive: true,
        plugins: {
            legend: {
                position: "top",
            },
            title: {
                display: true,
                text: "Memory usage over time (MB)"
            }
        }
        
    };

    return(
        <>
            <Stack spacing={5} justifyContent="space-between" width={"100%"}>
                <Line options={optionsCpu} data={graphDataCpu} />
                <Line options={optionsMemory} data={graphDataMemory} />
            </Stack>
            
        </>
    )
}


