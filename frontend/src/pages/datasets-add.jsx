import * as React from "react";
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
  OutlinedInput
} from "@mui/material";

import CheckIcon from '@mui/icons-material/Check';
import CloseIcon from '@mui/icons-material/Close';
import { Navigate } from "react-router-dom";
import Papa from "papaparse";


const allowedFileExts = ["csv", "json"];


export default function DatasetsAddPage(){

    const [userFileUpload, setUserFileUpload] = React.useState("");
    const [fileName, setFileName] = React.useState("");
    const [description, setDescription] = React.useState("");
    const [fileFormat, setFileFormat] = React.useState("");
    
    const [delim, setDelim] = React.useState("");
    const [hasHeader, setHasHeader] = React.useState(false);
    const [preview, setPreview] = React.useState([]);
    
    const [submitted, setSubmitted] = React.useState(false);
    const [datasetId, setDatasetId] = React.useState(null);
    const [datasetNames, setDatasetNames] = React.useState([]);
    const [loading, setLoading] = React.useState(true);
    const [isNameUnique, setIsNameUnique] = React.useState(true);

    React.useEffect(() => {

        async function fetchData() {
            try {
                setLoading(true);
                const fetchDatasets = await fetch("http://localhost:8081/datasets");
                const datasetsData = await fetchDatasets.json();
                const datasets = datasetsData._embedded?.datasetList || [];
                
                setDatasetNames(datasets.map((dataset) => (dataset.name)));   
            } 
            catch (error) {

            }
            finally {
                setLoading(false);
            }
        }

        fetchData();
    }, []);

    React.useEffect(() => {
        
        setIsNameUnique(!datasetNames.includes(fileName));

    }, [fileName, datasetNames]);


    const resetForm = () => {
        setUserFileUpload("");
        setFileName("");
        setDescription("");
        setFileFormat("");
        setDelim("");
        setHasHeader(false);
        setPreview([]);
    }

    const handleFileInputChange = (event) => {  
                
        resetForm();

        const file = event.target.files[0];
        if (!file) return;
        
        setUserFileUpload(file);

        const name = file.name;
        const ext = name.split(".").pop().toLowerCase().trim();
        const baseName = name.replace(/\.[^/.]+$/, "");
        
        if (ext === "csv") {
            readCsvFile(file);
        } else {
            console.error("This file extension is not allowed");
            alert("ERROR: This file extension is not allowed");
            return;
        }

        setFileName(baseName);
        setFileFormat(ext);
        setPreview([]);
        
    }

    const readCsvFile = (file) => {
        setPreview([]);

        Papa.parse(file, {
            header: false,  
            preview: 10,       
            skipEmptyLines: false,
            complete: (results) => {
                setPreview(results.data.slice(0, 10)); 
                setDelim(results.meta.delimiter);                
            },
        });
    }

    const handleSubmit = (event) => {
        event.preventDefault();

        let dataset = {
            name: fileName,
            description: description,
            fileFormat: fileFormat,
            delim: delim,
            header: hasHeader
        };
        
        let apiUrl = "http://127.0.0.1:8081/datasets";

        const formData = new FormData();

        formData.append(
            "dataset",
            new Blob([JSON.stringify(dataset)], { type: "application/json" })
        );

        const fileInput = document.querySelector("#file-select-input");
            if (fileInput.files.length > 0) {
            formData.append("file", fileInput.files[0], fileInput.files[0].name);
        }

        fetch(apiUrl, {
        method: "POST",
        body: formData,
        })
        .then((response) => {
            if (response.ok) {
                //alert("Dataset saved successfully", response);
                setSubmitted(true);
            return response.json();
            } 
            else if (response.status === 404) {
                throw new Error("Dataset not found");
            } 
            else if (response.status === 500) {
                throw new Error("Internal server error");
            } 
            else {
                throw new Error("Error saving Dataset");
            }
        })
        .catch((error) => {
            console.error(error);
            alert("An error occurred while saving the Dataset.");
        });
    }


    return (
        <>
            {submitted && <Navigate to="/datasets" replace={true} />}
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
            {/* Left side: form */}
            <Box sx={{ flex: 1, pr: 2 }}>                    
            <Stack
                component="form"
                onSubmit={(event) => handleSubmit(event)}
                spacing={3}
                justifyContent="center"
                alignItems="center"
            >
            {/* Select file */}
            <Tooltip title="Select the dataset to upload">
                <FormControl fullWidth required disabled={loading}>
                <FormLabel id="file-select-label">File</FormLabel>
                <Input 
                    id="file-select-input"
                    type="file" 
                    aria-labelledby="file-select-label"
                    name="userFileUpload" 
                    required   
                    onChange={handleFileInputChange}
                />                    
                </FormControl>
            </Tooltip>
            {/* Choose file name */}
            <Tooltip title="User given name of the dataset used as identifier. MUST BE UNIQUE">
                <FormControl fullWidth>
                
                <TextField
                    name="fileName"
                    label="File Name"
                    aria-labelledby="fileName-label"
                    value={fileName}
                    onChange={(e) => setFileName(e.target.value)}
                    required
                    slotProps={{
                        input: {
                        endAdornment: fileName.length > 0 ? (
                            <InputAdornment position="end">
                            {isNameUnique && fileName.length > 0 ? (
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
            <Tooltip title="User given description of the dataset, data origin, etc.">
                <FormControl fullWidth>
                <TextField
                    name="description"
                    label="Description of dataset"
                    aria-labelledby="description-label"
                    multiline
                    rows={3}                    
                    value={description}
                    onChange={(e) => setDescription(e.target.value)}
                />
                </FormControl>
            </Tooltip>
            {/* File Format input */}
            <Tooltip title="File format of the dataset. Only shown options are allowed." placement="top">
                <FormControl sx={{ m: 1, minWidth: 120 }} size="small">
                <InputLabel id="fileFormat-label">File format</InputLabel>
                <Select
                    name="fileFormat"
                    label="File Format"
                    labelId="fileFormat-label"
                    aria-labelledby="fileFormat-label"    
                    value={fileFormat}
                    onChange={(e) => setFileFormat(e.target.value)}
                    required
                >
                    <MenuItem value={"csv"}>CSV</MenuItem>
                </Select>
                </FormControl>
            </Tooltip>
            {fileFormat === "csv" && (
                <Stack 
                    direction="row" 
                    spacing={2} 
                    alignItems="flex-center" 
                    justifyContent="center"
                    sx={{ width: "100%" }}
                >
                    {/* Delimiter input */}
                    <Tooltip title="Delimiter used in the CSV dataset file. Default is comma (,)">
                        <FormControl sx={{ flex: 1 }}>
                        <TextField
                            name="delim"
                            value={delim}
                            label="Delimiter"
                            onChange={(e) => setDelim(e.target.value)}
                        />
                        </FormControl>
                    </Tooltip>
                    {/* Header checkbox */}
                    <Tooltip title="If checked, CSV contains header with column names">
                        <FormControl
                        sx={{ flex: 1, justifyContent: "center", alignItems: "center" }}                       
                        >
                        
                        <FormControlLabel
                            label="Header"
                            name="header"
                            size="small"
                            control={
                            <Checkbox
                                name="hasHeader"
                                checked={hasHeader}
                                onChange={(e) => setHasHeader(e.target.checked)}
                            />
                            }
                        />
                        </FormControl>
                    </Tooltip>
                </Stack>
            )}
            <Button variant="outlined" type="submit">
            {datasetId ? "Update" : "Submit"}
            </Button>
            </Stack>
            </Box>

            {/* Right side: file preview*/}
            <Box sx={{ flex: 1, pl: 2, borderLeft: "1px solid grey", overflowX:"auto" }}>
                <h3>File preview</h3>
                {preview && preview.length > 0 ? (
                  
                <Box sx={{ overflowX: "auto" , width: "100%", }}>
                <table style={{ borderCollapse: "collapse", width: "100%" }}>
                    
                    <>
                        <tbody>
                            {preview.map((row, i) => (
                                <tr key={i}>
                                {Object.values(row).map((cell, j) => (
                                    i===0 && hasHeader ? 
                                        (<td
                                        key={j}
                                        
                                        style={{
                                            border: "1px solid black",
                                            padding: "4px 8px",
                                            fontSize: "13px",
                                            background: "orange"
                                        }}
                                        
                                        
                                        >
                                        {cell}
                                        </td>
                                    )
                                    :(    
                                    <td
                                    key={j}
                                    
                                    style={{
                                        border: "1px solid black",
                                        padding: "4px 8px",
                                        fontSize: "13px",
                                    }}
                                    
                                    
                                    >
                                    {cell}
                                    </td>
                                    )
                                ))}
                                </tr>
                            ))}
                        </tbody>
                    </>
                        
                </table>
                </Box>
                ) : (
                    <span>No preview available. Select file.</span>
                )}
            </Box>

        </Box>
        </>
    );


}




