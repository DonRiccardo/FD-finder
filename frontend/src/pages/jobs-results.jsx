import React, { use, useEffect, useState } from "react";
import {
  Stack, Select, MenuItem, Box, Tab, Tabs,
  IconButton, Tooltip, FormControl, InputLabel,
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
import {
  Chart as ChartJS,
  CategoryScale, LinearScale, PointElement, LineElement,
  Title, Tooltip as ChartToolTip, Legend,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import { websocketListen } from "../utils/websocket-jobs";

const serverURL = import.meta.env.VITE_EUREKA_URL;
const jobServiceURL = import.meta.env.VITE_JOBSERVICE_URL;
const dataServiceURL = import.meta.env.VITE_DATASERVICE_URL;

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
        fdsMin: roundOrZero(stats.fdsMin),
        fdsAvg: roundOrZero(stats.fdsAvg),
        fdsMax: roundOrZero(stats.fdsMax),
    };
}

export default function JobsResults({ jobId }) {

    const [availableJobs, setAvailableJobs] = useState([]);
    const [loading, setLoading] = useState(true);
    const [fetchingResults, setFetchingResults] = useState(false);
    const [selectedTabValue, setSelectedTabValue] = useState(0);
    const [showFds, setShowFds] = useState(false);

    const [selectedJob, setSelectedJob] = useState("");
    const [selectedJobAlgorithm, setSelectedJobAlgorithm] = useState("");
    const [dataset, setDataset] = useState(null);
    const [jobStats, setJobStats] = useState(null);
    const [graphData, setGraphData] = useState(null);
    const [selectedIterationToShowFds, setSelectedIterationToShowFds] = useState("");
    const [foundFDs, setFoundFDs] = useState([]);

    const filteredResults = availableJobs[selectedJob]?.jobResults.filter(
        (r) => r.algorithm === selectedJobAlgorithm
    );

    const fetchResultsOfJob = async function () {
            
    if (!selectedJob) {
        resetForm();
        setFetchingResults(false);
        return;
    }
    const job = availableJobs[selectedJob];

    fetch(dataServiceURL+"/datasets/" + job.dataset)
    .then(res => {
        if (!res.ok) throw new Error("Failed to fetch dataset metadata");
        return res.json();
    })
    .then(datasetMetadata => {
        setDataset(datasetMetadata);

        if (job.status === "DONE") {
            return Promise.all([
                fetch(jobServiceURL+"/jobs/" + job.id + "/results")
                .then(r => { 
                    if (!r.ok) throw new Error("Failed to fetch job stats"); 
                    return r.json();
                }),
                fetch(jobServiceURL+"/jobs/" + job.id + "/results/graphdata")
                .then(r => { 
                    if (!r.ok) throw new Error("Failed to fetch graph data"); 
                    return r.json(); 
                })
            ])
            .then(([statsJson, graphJson]) => {
                const formattedStats = {};

                for (const [key, value] of Object.entries(statsJson || {})) {
                
                    formattedStats[key] = formatJobAlgorithmStats(value);
                }

                setJobStats(formattedStats);
                setGraphData(graphJson);
            });
        } 
        else {

            setJobStats(null);
            setGraphData(null);
        }
    })
    .catch((error) => {
        console.error("Error fetching job results or dataset:", error);
    })
    .finally(() => {
        setFetchingResults(false);
    });
}


    useEffect(() => {
        setLoading(true);
        websocketListen(setAvailableJobs);

        async function fetchJobs() {
            
            fetch(jobServiceURL+"/jobs")
            .then(response => {
                if (!response.ok) throw new Error("Failed to fetch jobs");
                return response.json();
            })
            .then(data => {
                const jobs = data._embedded?.jobList || [];
                const jobDict = {};
                jobs.forEach(job => {
                    jobDict[job.jobName] = {
                        ...job,
                    };
                });

                setAvailableJobs(jobDict);
            })
            .catch(error => {
                console.error("Error fetching jobs:", error);
            })
            .finally(() => {
                setLoading(false);
            });
        };
        
        fetchJobs();

    }, []);

    useEffect(() => {        
        if ((jobStats === null || graphData === null) && selectedJob && availableJobs[selectedJob].status === "DONE"){
            console.log("loading data after change of selected job");
            //setFetchingResults(true);
            //resetForm();             
            fetchResultsOfJob();
        }
    }, [availableJobs]);

    useEffect(() => {      
        if (jobId && availableJobs[Object.keys(availableJobs)[0]]) {
            const found = Object.values(availableJobs).find(j => j.id === parseInt(jobId, 10));
            if (found) setSelectedJob(found.jobName);        
        }
    }, [jobId, availableJobs]);

    useEffect(() => {
        setFetchingResults(true);
        resetForm();             
        fetchResultsOfJob();
    }, [selectedJob]);

    const handleShowFDs = async (jobResultId) => {
        
        fetch(jobServiceURL+"/jobs/results/" + jobResultId + "/fds")
        .then(res => { 
            if (!res.ok) throw new Error("Failed to fetch FDs"); return res.text(); 
        })
        .then(fdsRes => {
            const fdsArray = fdsRes.trim().split("\n").filter(line => line.trim() !== "")
            .map(fd => {
                const [lhsRaw, rhsRaw] = fd.split("->");
                const lhs = lhsRaw
                    .replace(/\[|\]/g, "")
                    .split(",")
                    .map(att => att.trim().replace(availableJobs[selectedJob].datasetName + ".", ""))
                    .join(",");
                const rhs = rhsRaw.trim().replace(availableJobs[selectedJob].datasetName + ".", "");
                return { lhs, rhs };
            });

            setFoundFDs(fdsArray);
            setShowFds(true);
            setSelectedIterationToShowFds(jobResultId);
        })
        .catch(error => {
            console.error("Error fetching fds:", error);
        })
        .finally(() => {
            setLoading(false);
        });
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

    const handleDownload = async (iterationId) => {
        if (!Number.isInteger(iterationId)){
            alert("Invalid iteration ID.");
            return;
        }
        
        fetch(jobServiceURL+"/jobs/results/" + iterationId + "/fds")
        .then(response => {
            if (!response.ok) throw new Error("Failed to download found FDs");
            return response.blob().then(blob => ({ response, blob }));
        })
        .then(({ response, blob }) => {
            const disposition = response.headers.get("Content-Disposition") || response.headers.get("content-disposition");
            let filename = "job-" + availableJobs[selectedJob].id + "-" + selectedJobAlgorithm + "-runid-" + iterationId + "-foundFDs.txt";

            if (disposition && disposition.includes("filename")) {
                const match = disposition.match(/filename="?([^";]+)"?/);
                if (match) filename = match[1];
            }

            const url = window.URL.createObjectURL(blob);
            const link = document.createElement('a');

            link.href = url;
            link.download = filename;
            document.body.appendChild(link);
            link.click();
            link.remove();
            window.URL.revokeObjectURL(url);
        })
        .catch(error => {
            console.error("Error downloading found FDs:", error);
            alert(`Error downloading found FDs of job ${availableJobs[selectedJob]?.id ?? "?"}.`);
        });   
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
                    {Object.keys(availableJobs).map((name) => ( 
                        <MenuItem key={name} value={name}>{name}</MenuItem>
                    ))}
                </Select>
            </FormControl>

            {/* Details */}
            {loading ? 
                <div> <CircularProgress /> Loading data... </div> 
                : 
                <>
                    {selectedJob ? 
                        <>
                        <Stack spacing={5} direction="row" justifyContent="space-between" width={"100%"}>
                            <h3 style={{marginLeft: 5}}>{selectedJob}</h3> 
                            <span
                                style={{ fontWeight: "bold", textAlign: "right",
                                    backgroundColor: availableJobs[selectedJob].status === "DONE" ? "green" : 
                                           availableJobs[selectedJob].status === "RUNNING" ? "blue" :
                                           availableJobs[selectedJob].status === "CANCELED" ? "orange" :
                                           availableJobs[selectedJob].status === "FAILED" ? "red" :
                                           "black",
                                    color: "white", padding: "5px", borderRadius: "8px"
                                 }}
                            >
                                {availableJobs[selectedJob].status}
                            </span>
                        </Stack>
                                                    
                            
                         <List sx={{ ...styleList}}>
                            <ListItem>
                                <ListItemText primary="Job ID:" 
                                secondary={availableJobs[selectedJob].id}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                                <ListItemText primary="Job Name:" 
                                secondary={selectedJob}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                                <ListItemText primary="Job Description:" 
                                secondary={availableJobs[selectedJob].jobDescription || "No description"}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                                <ListItemText primary="Algorithm:" 
                                secondary={availableJobs[selectedJob].algorithm.join(", ") || ""}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                                <ListItemText primary="Dataset:" 
                                secondary={dataset ? (`${availableJobs[selectedJob].datasetName} (Attributes: ${dataset.numAttributes}, Entries: ${dataset.numEntries})`) : ("Dataset not found!")}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                               <ListItemText primary="Repeat:" 
                                secondary={availableJobs[selectedJob].repeat}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                               <ListItemText primary="LIMIT entries:" 
                                secondary={availableJobs[selectedJob].limitEntries > 0 ? availableJobs[selectedJob].limitEntries : "undefined"}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                               <ListItemText primary="SKIP entries:" 
                                secondary={availableJobs[selectedJob].skipEntries > 0 ? availableJobs[selectedJob].skipEntries : "undefined"}
                                slotProps={{
                                    secondary: { sx: {  textAlign: "right" } },
                                }}
                                />
                            </ListItem>
                            <Divider variant="middle" component="li" />
                            <ListItem>
                               <ListItemText primary="MAX LHS:" 
                                secondary={availableJobs[selectedJob].maxLHS >= 0 ? availableJobs[selectedJob].maxLHS : "undefined"}
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
            <Box sx={{ width: "100%", maxHeight: 800 , flex: 1, pl: 2, borderLeft: "1px solid grey", overflowX:"auto", overflowY:"auto" }}>
                
                { selectedJob != null && selectedJob !== "" && availableJobs[selectedJob].status === "DONE" && jobStats !== null && graphData !== null ?
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
                                onChange={(e) => {
                                    setSelectedJobAlgorithm(e.target.value);
                                    handleBack();
                                }}                   
                            >
                                {availableJobs[selectedJob].algorithm.map((alg) => ( 
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
                <>{fetchingResults ? <div> <CircularProgress /> Loading data... </div> : <p>Nothing to show</p>}</>
                
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
                secondary={stats.fdsMin + " / " + stats.fdsAvg + " / " + stats.fdsMax}
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

    const keys = Object.keys(data || {});
    const total = keys.length;

    const xLabels = [];
    const graphDataCpu = {
        labels: xLabels,
        datasets: []
    }
    const graphDataMemory = {
        labels: xLabels,
        datasets: []
    }

    keys.forEach((key, idx) =>  {
        const value = data[key];

        const cpuData = [];
        const memoryData = [];

        const color = generateColor(idx, total);

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
                borderColor: color,
                backgroundColor: color
            }
        )
    

        graphDataMemory.datasets.push(
            {
                label: key,
                data: memoryData,
                borderColor: color,
                backgroundColor: color,
            }
        )

    });


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

function generateColor(index, total) {
  const hue = (index * 360 / Math.max(1, total)) % 360;
  return `hsl(${hue}, 70%, 50%)`;
}
