import React, { useEffect, useState } from "react";
import {
  Stack,
  Input,
  TextField,
  Button,
  Checkbox,
  Select,
  MenuItem,
  Box,
  ListItemText,
  IconButton,
  Tooltip,
  InputAdornment,
  FormControl, FormControlLabel, FormLabel, InputLabel
} from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { Navigate } from "react-router-dom";
import CheckIcon from '@mui/icons-material/Check';
import CloseIcon from '@mui/icons-material/Close';



export default function JobsCreate() {

    const [availableAlgorithms, setAvailableAlgorithms] = useState([]);
    const [availableDatasets, setAvailableDatasets] = useState([]);
    const [loading, setLoading] = useState(true);
    const [jobNames, setJobNames] = useState([]);

    const [jobName, setJobName] = useState("");
    const [jobDescription, setJobDescription] = useState("");
    const [algorithm, setAlgorithm] = useState([]);
    const [dataset, setDataset] = useState({id: "", name: "", entries: 0});
    const [repeat, setRepeat] = useState(1);
    const [limitEntries, setLimitEntries] = useState("");
    const [skipEntries, setSkipEntries] = useState("");
    const [maxLhs, setMaxLhs] = useState("");
    const [submitted, setSubmitted] = useState(false);

    const [isNameUnique, setIsNameUnique] = React.useState(true);

    
    React.useEffect(() => {
        
        setIsNameUnique(!jobNames.includes(jobName.trim()));

    }, [jobName, jobNames]);
    
    
    useEffect(() => {
        async function fetchData() {
            try {
                const fetchAlgorithms = await fetch("http://localhost:8761/algorithms");                
                const algorithmsData = await fetchAlgorithms.json();
                setAvailableAlgorithms(algorithmsData);

                const fetchDatasets = await fetch("http://localhost:8081/datasets");
                const datasetsData = await fetchDatasets.json();
                const datasets = datasetsData._embedded?.datasetList || [];

                setAvailableDatasets(datasets.map((dataset) => ({
                    id: dataset.id,
                    name: dataset.name,
                    entries: dataset.numEntries,
                })));    
                
                const fetchJobs = await fetch("http://localhost:8082/jobs");
                const jobsData = await fetchJobs.json();
                const jobs = jobsData._embedded?.jobList || [];
                setJobNames(jobs.map((job) => job.jobName));

            } 
            catch (error) {
                
            }
            finally {
                setLoading(false);
            }
        };

        fetchData();
    }, []);

    const handleAlgorithmChange = (event) => {
        const {
        target: { value },
        } = event;
        setAlgorithm(
            // On autofill we get a stringified value.
            typeof value === 'string' ? value.split(',') : value,
        );
    };

    const handleSubmit = async (event) => {
        event.preventDefault();

        const jobData = {
            jobName: jobName,
            jobDescription: jobDescription,
            algorithm: algorithm,
            dataset: dataset.id,
            datasetName: dataset.name,
            repeat: repeat,
        };

        if (limitEntries !== "" && limitEntries !== undefined && !isNaN(limitEntries) && limitEntries > 0) {
            jobData.limitEntries = limitEntries;
        }
        if (skipEntries !== "" && skipEntries !== undefined && !isNaN(skipEntries) && skipEntries > 0) {
            jobData.skipEntries = skipEntries;
        }
        if (maxLhs !== "" && maxLhs !== undefined && !isNaN(maxLhs) && maxLhs >= 0) {
            jobData.maxLHS = maxLhs;
        }

        try {
            const response = await fetch("http://localhost:8082/jobs", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(jobData),
            });

            if (response.ok) {
                setSubmitted(true);
            }
            else {
                console.error("Failed to create job");
                alert("Failed to create job. Please check the input data and try again.");
            }
        }
        catch (error) {
            console.error("Error creating job:", error);
            alert("Error creating job. Please try again later.");
        }

    }

    const resetForm = () => {
        setAlgorithm([]);
        setDataset({id: "", name: "", entries: 0});
        setLimitEntries("");
        setSkipEntries("");
        setMaxLhs("");
        setRepeat(1);
    }
    
    return (
        <Box 
            sx={{
                    width: "50%",
                    margin: "auto auto 15px auto",
                    padding: "1%",
                    border: "1px solid black",
                    borderRadius: "16px",
                    minHeight: "400px",
            }}
        >
        <div>
        {submitted && <Navigate to="/jobs" replace={true} />}        
            <Stack 
                component="form"
                onSubmit={(event) => handleSubmit(event)}
                onReset={() => resetForm()}
                spacing={3}
                justifyContent="center"
                alignItems="center"
                >
                {/* Job name */}
                <Tooltip title="Name of the job used as user-friendly identifier. MUST BE UNIQUE">
                    <FormControl fullWidth>                    
                    <TextField
                        name="jobName"
                        label="Job Name"
                        aria-labelledby="jobName-label"
                        value={jobName}
                        onChange={(e) => setJobName(e.target.value)}
                        required
                        error={!isNameUnique}
                        helperText={!isNameUnique ? "Job name must be unique" : ""}
                        slotProps={{
                            input: {
                            endAdornment: jobName.length > 0 ? (
                                <InputAdornment position="end">
                                {isNameUnique && jobName.length > 0 ? (
                                    <CheckIcon color="success" />
                                ) : (
                                    <CloseIcon color="error" />
                                )}
                                </InputAdornment>
                            ) : null,
                            },
                        }}
                    />
                    </FormControl>
                </Tooltip>
                {/* Description input */}
                <Tooltip title="Description of the job, meaning, idea you are checking.">
                    <FormControl fullWidth>
                    <TextField
                        name="description"
                        label="Description of job"
                        aria-labelledby="description-label"
                        multiline
                        rows={3}                    
                        value={jobDescription}
                        onChange={(e) => setJobDescription(e.target.value)}
                    />
                    </FormControl>
                </Tooltip>
                {/* Selection of algorithm to perform FDs finding */}
                <Tooltip title="Select one or more algorithms to find functional dependencies.">
                    <FormControl fullWidth required disabled={loading}>
                        <InputLabel id="algorithm-label">Algorithm</InputLabel>
                        <Select
                            labelId="algorithm-label"
                            value={algorithm}
                            label="Algorithm"
                            multiple
                            renderValue={(selected) => selected.join(', ')}
                            onChange={handleAlgorithmChange}                        
                        >
                            {availableAlgorithms.map((alg) => ( 
                                <MenuItem key={alg} value={alg}>
                                    <Checkbox checked={algorithm.includes(alg)} />
                                    <ListItemText primary={alg} />
                                </MenuItem>
                            ))}
                        </Select>
                    </FormControl>
                </Tooltip>
                {/* Select dataset */}
                <Tooltip title="Select the dataset on which the job will be performed.">
                    <FormControl fullWidth required disabled={loading}>
                        <InputLabel id="dataset-label">Dataset</InputLabel>
                        <Select
                            labelId="dataset-label"
                            value={dataset}
                            label="Dataset" 
                            onChange={(e) => setDataset(e.target.value)}
                        >
                            {availableDatasets.map((ds) => (    
                                <MenuItem key={ds.id} value={ds}>{ds.name}</MenuItem>
                            ))}
                        </Select>
                    </FormControl>
                </Tooltip>
                {/* MAX number of rows OR entries to process */}
                <Tooltip title="Maximum number of entries to process from the dataset. If not set, all entries will be processed.">
                    <TextField
                        label="Limit Entries"
                        type="number"                    
                        value={limitEntries}
                        onChange={(e) => setLimitEntries(parseInt(e.target.value, 10))}
                        size="small"
                        error = {limitEntries < 0 || limitEntries > dataset.entries}
                        helperText = {limitEntries < 0 || limitEntries > dataset.entries ? `Must be between 0 and ${dataset.entries}` : ""}
                        />
                </Tooltip>
                {/* NUmber of rows OR entries to SKIP from beginig of the dataset */}
                <Tooltip title="Number of entries to skip from the beginning of the dataset.">
                    <TextField
                        label="Skip Entries"
                        type="number"
                        value={skipEntries}
                        onChange={(e) => setSkipEntries(parseInt(e.target.value, 10))}
                        size="small"
                        error = {skipEntries < 0 || skipEntries > dataset.entries}
                        helperText = {skipEntries < 0 || skipEntries > dataset.entries ? `Must be between 0 and ${dataset.entries}` : ""}
                        />  
                </Tooltip>
                {/* MAX size of LHS */}
                <Tooltip title="Maximum size of the left-hand side (LHS) of the functional dependencies to be discovered.">
                    <TextField
                        label="Max LHS"
                        type="number"
                        value={maxLhs}
                        onChange={(e) => setMaxLhs(parseInt(e.target.value, 10))}
                        size="small"
                        error = {maxLhs < 0 }
                        helperText = {maxLhs < 0 ? "Must be non-negative" : ""}
                        />  
                </Tooltip>
                {/* Number of repetition computation of each selected algorithm */}
                <Tooltip title="Number of times to repeat the FD finding process for each selected algorithm. Must be between 1 and 20.">
                    <TextField
                        label="Repeat"
                        type="number"
                        value={repeat}
                        onChange={(e) => setRepeat(e.target.value)}
                        size="small"    
                        error = {repeat < 1 || repeat > 20 }
                        helperText = {repeat < 1 || repeat > 20 ? `Must be between 0 and 20` : ""}
                        />
                </Tooltip>
                <Stack spacing={5} direction="row">
                    <Button type="submit" variant="contained">Create Job</Button>
                    <Button type="reset" variant="outlined" >Cancel</Button>
                </Stack>
            </Stack>
        
        </div>
        </Box>   

    )

}



