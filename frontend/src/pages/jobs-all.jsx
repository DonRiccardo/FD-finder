import React, { useEffect, useState } from "react";
import { Button, Box, Stack, IconButton } from "@mui/material";
import {
    DataGrid, Toolbar, ToolbarButton, ColumnsPanelTrigger,
    FilterPanelTrigger, GridActionsCellItem
} from "@mui/x-data-grid";
import { Link } from "react-router-dom";
import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from '@mui/icons-material/Delete';
import FindInPageIcon from '@mui/icons-material/FindInPage';
import ViewColumnIcon from '@mui/icons-material/ViewColumn';
import FilterListIcon from '@mui/icons-material/FilterList';
import Tooltip from '@mui/material/Tooltip';
import CancelIcon from '@mui/icons-material/Cancel';
import Badge from '@mui/material/Badge';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import { websocketListen, formatJobMetadaata } from "../utils/websocket-jobs";

const jobServiceURL = import.meta.env.VITE_JOBSERVICE_URL;

export default function JobssAll() {

    const [pageSize, setPageSize] = useState(20);
    const [rows, setRows] = useState([]);

    const fetchJobs = React.useCallback(async () => {
        
        fetch(jobServiceURL+"/jobs")
        .then((response) => {
            if(!response.ok) Promise.reject(response);
            return response.json();
        })
        .then((data) => {

            const jobs= data._embedded?.jobList || [];

            const formatted = jobs.map((job) => (formatJobMetadaata(job)));

            setRows(formatted);
        })
        .catch((error) => {
            console.error("Error fetching datasets:", error);
        });            
        
    }, []);

    useEffect(() => {
        fetchJobs();
    }, [fetchJobs]);

    useEffect(() => {
        websocketListen(setRows);
    }, []);

    const runJob = (row) => async () => {
        if (row.canRun) {
            fetch(`${jobServiceURL}/jobs/${row.id}/start`, {  
                method: 'POST',
            })
            .then((response) => {
                if (response.ok) {
                    fetchJobs(); 
                } else {
                    alert(`Failed to start job "${row.id}".`);
                }
            })
            .catch((error) => {
                console.error("Error starting job:", error);
                alert(`Error starting job "${row.id}".`);
            });
        }
    }

    const cancelJob = (row) => async () => {
        if (row.canCancel) {
            fetch(`${jobServiceURL}/jobs/${row.id}/cancel`, {  
                method: 'DELETE', 
            })
            .then((response) => {
                if (response.ok) {
                    fetchJobs();
                } else {
                    alert(`Failed to cancel job "${row.id}".`);
                }   
            })
            .catch((error) => {
                console.error("Error cancelling job:", error);
                alert(`Error cancelling job "${row.id}".`);
            });
        }
    }
    

    const columns = [
        {
            field: "actions",
            type: "actions",
            minWidth: 60,
            getActions: (params) => [
                <Tooltip title="Run Job">
                <GridActionsCellItem
                    icon={<PlayArrowIcon sx={{ fontSize: 35 }} />}
                    disabled={!params.row.canRun}
                    onClick={runJob(params.row)}
                    sx={{ color: params.row.canRun ? "green" : "primary" }}
                />
                </Tooltip>,
                <Tooltip title="Cancel Job">
                <GridActionsCellItem
                    icon={<CancelIcon sx={{ fontSize: 25 }}/>}
                    disabled={!params.row.canCancel}
                    onClick={cancelJob(params.row)}
                    sx={{ color: params.row.canCancel ? "red" : "primary" }}
                />
                </Tooltip>
            ]
        },
        { field: "id", headerName: "ID", width: 10, type: "number" },
        { field: "jobName", headerName: "Name", width: 100 },
        { field: "jobDescription", headerName: "Description", minWidth: 100 },
        { field: "algorithm", headerName: "Algorithm", minWidth: 20 },
        { field: "datasetName", headerName: "Dataset Name", minWidth: 150 },
        { field: "status", headerName: "Status", minWidth: 40 },    
        { field: "limitEntries", headerName: "LIMIT REC", minWidth: 50, type: "number",
            valueGetter: (value) => {
                return value > 0 ? value : "-";
            }
         },
        { field: "skipEntries", headerName: "SKIP REC", minWidth: 50, type: "number",
            valueGetter: (value) => {
                return value > 0 ? value : "-";
            }
         },
        { field: "maxLHS", headerName: "MAX LHS", minWidth: 10, type: "number", 
            valueGetter: (value) => {
                return value > 0 ? value : "-";
            }
         },        
       {
        field: "actions2",
        type: "actions",
        minWidth: 180,
        renderCell: (params) => (
            <>
                <Link to={"/jobs/results/" + params.row.id}>
                    <Tooltip title="Show details & results  ">
                    <IconButton variant="outlined" size="small" sx={{ mr: 1 }} aria-label="create job">
                        <FindInPageIcon 
                            aria-label="show results"
                            onClick={() => {}}
                        />
                    </IconButton>
                    </Tooltip>
                </Link>
                <Tooltip title={params.row.canDelete ? "Delete Job" : "Delete Disabled"}>
                    <span>
                    <IconButton 
                        aria-label="delete"
                        variant="outlined" 
                        size="small"
                        color="error"
                        sx={{ mr: 1 }}
                        disabled={!params.row.canDelete}
                        onClick={() => handleDelete(params.row)}
                    >
                        <DeleteIcon />
                    </IconButton>
                    </span>                
                </Tooltip>
            </>
        )
       },
       
        { field: "createdAt", 
            headerName: "Created At", 
            minWidth: 150,
            type: "dateTime"
        },        
        { field: "updatedAt", 
            headerName: "Updated At", 
            width: 150,
            type: "dateTime",
        },
    ];

    const handleDelete = (row) => {
        if (row.canDelete) {
            if (window.confirm(`Are you sure you want to delete job "${row.id}"? This action cannot be undone.`)) {
                fetch(`${jobServiceURL}/jobs/${row.id}/delete`, {
                    method: 'DELETE',
                })
                .then((response) => {
                    if (response.ok) {
                        alert(`Job "${row.id}" deleted successfully.`);
                        fetchJobs(); 
                    } else {
                        alert(`Failed to delete job "${row.id}".`);
                    }
                })
                .catch((error) => {
                    console.error("Error deleting job:", error);
                    alert(`Error deleting job "${row.id}".`);
                });

            }
        }
    }
   


    return (
        <>
            <Box sx={{ 
                height: "100%", 
                width: "90%", 
                margin: "auto auto", 
                justifyContent: "center",
                alignItems: "center",    
                }}
            >
                <Stack direction="column" spacing={2}>
                    <Box sx={{ height: 550, flexGrow: 1 }}>
                        <DataGrid
                            rows={rows}
                            columns={columns}
                            pageSize={pageSize}
                            onPageSizeChange={(newPageSize) => setPageSize(newPageSize)}
                            pageSizeOptions={[10, 20, 50, 100]}
                            pagination
                            slots={{ toolbar: EditToolbar }}
                            showToolbar
                            />
                    </Box>
                </Stack>
            </Box>
        </>
    );
}


function EditToolbar() {
  return (
    <Toolbar>
        <Link to={"/jobs/create"}>
            <Button color="secondary" variant="outlined" startIcon={<AddIcon />}>
                Add Job
            </Button>
        </Link>
        <Tooltip title="Columns">
            <ColumnsPanelTrigger render={<ToolbarButton />}>
                <ViewColumnIcon fontSize="small" />
            </ColumnsPanelTrigger>
        </Tooltip>
        <Tooltip title="Filters">
            <FilterPanelTrigger
            render={(props, state) => (
                <ToolbarButton {...props} color="default">
                <Badge badgeContent={state.filterCount} color="primary" variant="dot">
                    <FilterListIcon fontSize="small" />
                </Badge>
                </ToolbarButton>
            )}
            />
        </Tooltip>

      
    </Toolbar>
  );
}
