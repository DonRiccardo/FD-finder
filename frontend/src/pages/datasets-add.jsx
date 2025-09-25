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
  FormControl, FormControlLabel, FormLabel
} from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { Navigate } from "react-router-dom";
import Papa from "papaparse";


const allowedFileExts = ["csv", "json"];


export default class DatasetsAddPage extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            userFileUpload: "",
            fileName: "",
            description: "",
            fileFormat: "",
            delim: "",
            hasHeader: false,
            preview: [],
        };

        this.handleInputChange = this.handleInputChange.bind(this);
        this.handleFileNameInputChange = this.handleFileNameInputChange.bind(this);
        this.handleSubmit = this.handleSubmit.bind(this);
        this.handleHeaderInputChange = this.handleHeaderInputChange.bind(this);
    }

    handleInputChange(event) {
        const target = event.target;
        const value = target.value;
        const name = target.name;

        this.setState({
        [name]: value,
    });
    }

    resetForm(callback) {
        this.setState({
            userFileUpload: "",
            fileName: "",
            description: "",
            fileFormat: "",
            delim: "",
            hasHeader: false,
            preview: [],
        }, callback);
    }

    handleFileNameInputChange(event) {   
        
        this.resetForm(() => {
            const file = event.target.files[0];
            if (!file) return;

            this.setState({ userFileUpload: file });

            const name = file.name;
            const ext = name.split(".").pop().toLowerCase().trim();
            const baseName = name.replace(/\.[^/.]+$/, "");
            console.log("Selected file:", name, "with extension:", ext);
            if (ext === "csv") {
                this.readCsvFile(file);
            } else {
                console.error("This file extension is not allowed");
                alert("ERROR: This file extension is not allowed");
                return;
            }

            this.setState({ fileName: baseName });
            this.setState({ fileFormat: ext });
            this.setState({ preview: [] });
        });
    }

    readCsvFile(file) {
        this.setState({ preview: [] }); 
        Papa.parse(file, {
            header: this.hasHeader,        
            preview: 10,        
            skipEmptyLines: true,
            complete: (results) => {
                this.setState({ preview: results.data });
                this.setState({ delim: results.meta.delimiter });                 
            },
        });
    }

    handleHeaderInputChange(event) {
        this.setState({
            [event.target.name]: event.target.checked,
        });

        this.readCsvFile(this.state.userFileUpload);
    }

    handleSubmit(event) {
        event.preventDefault();

        const { fileName, description, fileFormat, delim, hasHeader, preview, submitted } = this.state;
        const { datasetId } = this.props;

        let dataset = {
            name: fileName,
            description: description,
            fileFormat: fileFormat,
            delim: delim,
            header: hasHeader
        };
        console.log("Submitting dataset:", dataset);
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
                this.setState({ submitted: true });
            return response.json();
            } else if (response.status === 404) {
            throw new Error("Dataset not found");
            } else if (response.status === 500) {
            throw new Error("Internal server error");
            } else {
            throw new Error("Error saving Dataset");
            }
        })
        .catch((error) => {
            console.error(error);
            alert("An error occurred while saving the Dataset.");
        });
    }

    render() {
        const { fileName, description, fileFormat, delim, hasHeader, preview, submitted } = this.state;
        const { datasetId } = this.props;

        return (
            <>
               {submitted && <Navigate to="/datasets" replace={true} />}
                <Box
                sx={{
                    width: "80%",
                    margin: "auto auto",
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
                    onSubmit={(event) => this.handleSubmit(event)}
                    spacing={3}
                    justifyContent="center"
                    alignItems="center"
                >
                {/* Select file */}
                <FormControl fullWidth>
                <FormLabel id="file-select-label">File</FormLabel>
                <Input 
                    id="file-select-input"
                    type="file" 
                    aria-labelledby="file-select-label"
                    name="userFileUpload" 
                    required   
                    onChange={this.handleFileNameInputChange}                     
                    slotProps={{
                        input: {
                        endAdornment: (
                            <HelpTooltip
                                title="Select the dataset to upload"
                                adorned
                            />
                        ),
                        },
                    }}
                />                    
                </FormControl>
                {/* Choose file name */}
                <FormControl fullWidth>
                <FormLabel id="fileName-label">Name</FormLabel>
                <TextField
                    name="fileName"
                    aria-labelledby="fileName-label"
                    value={fileName}
                    onChange={this.handleFileNameInputChange}
                    required
                    slotProps={{
                        input: {
                        endAdornment: (
                            <HelpTooltip
                                title="User given name of the dataset used as identifier"
                                adorned
                            />
                        ),
                        },
                    }}
                />
                </FormControl>
                {/* Description input */}
                <FormControl fullWidth>
                <FormLabel id="description-label">
                    Description of dataset
                </FormLabel>
                <TextField
                    name="description"
                    aria-labelledby="description-label"
                    multiline
                    rows={3}                    
                    value={description}
                    onChange={this.handleInputChange}
                    slotProps={{
                        input: {
                        endAdornment: (
                            <HelpTooltip
                                title="User given description of the dataset, data origin, etc."
                                adorned
                            />
                        ),
                        },
                    }}
                />
                </FormControl>
                {/* File Format input */}
                <FormControl>
                <FormLabel id="fileFormat-label">File format</FormLabel>
                <Select
                    name="fileFormat"
                    aria-labelledby="fileFormat-label"                        
                    size="small"
                    value={fileFormat}
                    onChange={this.handleInputChange}
                    required
                    slotProps={{
                        input: {
                        endAdornment: (
                            <HelpTooltip
                                title="File format of the dataset. Only shown options are allowed."
                                adorned
                            />
                        ),
                        },
                    }}
                >
                    <MenuItem value={"csv"}>CSV</MenuItem>
                </Select>
                </FormControl>
                {fileFormat === "csv" && (
                    <Stack 
                        direction="row" 
                        spacing={2} 
                        alignItems="flex-center" 
                        justifyContent="center"
                        sx={{ width: "100%" }}
                    >
                        {/* Delimiter input */}
                        <FormControl sx={{ flex: 1 }}>
                        <FormLabel id="delim-label">Delimiter</FormLabel>
                        <TextField
                            name="delim"
                            aria-labelledby="delim-label"
                            value={delim}
                            onChange={this.handleInputChange}
                            slotProps={{
                                input: {
                                endAdornment: (
                                    <HelpTooltip
                                        title="Delimiter used in the CSV dataset file. Default is comma (,)"
                                        adorned
                                    />
                                ),
                                },
                            }}
                        />
                        </FormControl>
                        {/* Header checkbox */}
                        <FormControl
                         sx={{ flex: 1, justifyContent: "center", alignItems: "center" }}
                        >
                        <FormLabel id="header-label">Header</FormLabel>
                        <FormControlLabel
                            aria-labelledby="header-label"
                            name="header"
                            size="small"
                            label={
                            <HelpTooltip title="If checked, CSV contains header with column names" />
                            }
                            control={
                            <Checkbox
                                name="hasHeader"
                                value={hasHeader}
                                onChange={this.handleHeaderInputChange}
                            />
                            }
                        />
                        </FormControl>
                    </Stack>
                )}
                <Button variant="outlined" type="submit">
                {datasetId ? "Update" : "Submit"}
                </Button>
                </Stack>
                </Box>

                {/* Right side: file preview*/}
                <Box sx={{ flex: 1, pl: 2, borderLeft: "1px solid grey" }}>
                    <h3>File preview</h3>
                    {preview && preview.length > 0 ? (
                    <Box sx={{ overflowX: "auto" , width: "100%", }}>
                    <table style={{ borderCollapse: "collapse", width: "100%" }}>
                        
                        <>
                            <tbody>
                                {preview.map((row, i) => (
                                    <tr key={i}>
                                    {Object.values(row).map((cell, j) => (
                                        
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

}



function HelpTooltip(props) {
  const IconTooltip = () => {
    return (
      <Tooltip title={props.title}>
        <IconButton>
          <HelpOutlineIcon />
        </IconButton>
      </Tooltip>
    );
  };

  if (props.adorned) {
    return <InputAdornment position="end">{IconTooltip()}</InputAdornment>;
  } else {
    return IconTooltip();
  }
}

