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
  IconButton,
  Tooltip,
  InputAdornment,
  FormControl, FormControlLabel, FormLabel, InputLabel
} from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { Navigate } from "react-router-dom";




export default function JobsCreate() {

    const [availableAlgorithms, setAvailableAlgorithms] = useState([]);
    const [availableDatasets, setAvailableDatasets] = useState([]);
    const [loading, setLoading] = useState(true);

    const [algorithm, setAlgorithm] = useState("");
    const [dataset, setDataset] = useState("");
    const [limitEntries, setLimitEntries] = useState(0);
    const [skipEntries, setSkipEntries] = useState(0);
    const [maxLhs, setMaxLhs] = useState(0);
    const [output, setOutput] = useState("");
    const [submitted, setSubmitted] = useState(false);

    

    
    
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
                })));                

            } 
            catch (error) {
                
            }
            finally {
                setLoading(false);
            }
        };

        fetchData();
    }, []);

    const handleSubmit = async (event) => {
        event.preventDefault();

        const jobData = {
            algorithm: algorithm,
            dataset: dataset,
            limitEntries: limitEntries,
            skipEntries: skipEntries,
            maxLHS: maxLhs,
            output: output,
        };

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
                spacing={3}
                justifyContent="center"
                alignItems="center"
                >
                <FormControl fullWidth required disabled={loading}>
                    <InputLabel id="algorithm-label">Algorithm</InputLabel>
                    <Select
                        labelId="algorithm-label"
                        value={algorithm}
                        //label="Algorithm"
                        onChange={(e) => setAlgorithm(e.target.value)}                        
                    >
                        {availableAlgorithms.map((alg) => ( 
                            <MenuItem key={alg} value={alg}>{alg}</MenuItem>
                        ))}
                    </Select>
                </FormControl>
                <FormControl fullWidth required disabled={loading}>
                    <InputLabel id="dataset-label">Dataset</InputLabel>
                    <Select
                        labelId="dataset-label"
                        value={dataset}
                        label="Dataset" 
                        onChange={(e) => setDataset(e.target.value)}
                    >
                        {availableDatasets.map((ds) => (    
                            <MenuItem key={ds.id} value={ds.id}>{ds.name}</MenuItem>
                        ))}
                    </Select>
                </FormControl>
                <TextField
                    label="Limit Entries"
                    type="number"
                    value={limitEntries}
                    onChange={(e) => setLimitEntries(parseInt(e.target.value, 10))}
                    size="small"
                    />
                <TextField
                    label="Skip Entries"
                    type="number"
                    value={skipEntries}
                    onChange={(e) => setSkipEntries(parseInt(e.target.value, 10))}
                    size="small"
                    />  
                <TextField
                    label="Max LHS"
                    type="number"
                    value={maxLhs}
                    onChange={(e) => setMaxLhs(parseInt(e.target.value, 10))}
                    size="small"
                    />  
                <TextField
                    label="Output"
                    type="text"
                    value={output}
                    onChange={(e) => setOutput(e.target.value)}
                    size="small"    
                    />
                <Button type="submit" variant="contained">Create Job</Button>
            </Stack>
        
        </div>
        </Box>   

    )

}



