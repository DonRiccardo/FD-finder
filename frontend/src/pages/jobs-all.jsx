import React, { useEffect, useState } from "react";
import { Button, Box, Stack, IconButton } from "@mui/material";
import {
    DataGrid,
    Toolbar,
    ToolbarButton,
    ColumnsPanelTrigger,
    FilterPanelTrigger,
    GridActionsCellItem
} from "@mui/x-data-grid";
import { Link } from "react-router-dom";
import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import DownloadIcon from '@mui/icons-material/Download';
import FindInPageIcon from '@mui/icons-material/FindInPage';
import ViewColumnIcon from '@mui/icons-material/ViewColumn';
import FilterListIcon from '@mui/icons-material/FilterList';
import Tooltip from '@mui/material/Tooltip';
import CancelIcon from '@mui/icons-material/Cancel';
import SearchIcon from '@mui/icons-material/Search';
import Typography from '@mui/material/Typography';
import Badge from '@mui/material/Badge';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';


export default function JobssAll() {

    const [pageSize, setPageSize] = useState(20);
    const [rows, setRows] = useState([]);

    const fetchJobs = React.useCallback(async () => {
        try {
            fetch("http://localhost:8082/jobs")
            .then((response) => response.json())
            .then((data) => {

                const jobs= data._embedded?.jobList || [];

                const formatted = jobs.map((job) => ({
                    ...job,
                    canDelete: Boolean(job._links?.delete),
                    canCancel: Boolean(job._links?.cancel),
                    canRun: Boolean(job._links?.start),
                    createdAt: job.createdAt ? new Date(job.createdAt.replace(/(\.\d{3})\d+/, "$1")) : null,
                    updatedAt: job.updatedAt ? new Date(job.updatedAt.replace(/(\.\d{3})\d+/, "$1")) : null,
                }));

                setRows(formatted);
            })
            .catch((error) => {
                console.error("Error fetching datasets:", error);
            });
            
        }
        catch (error) {
            console.error("Error fetching datasets:", error);
        }
    }, []);

    useEffect(() => {
        fetchJobs();
    }, [fetchJobs]);

    const runJob = (row) => async () => {
        if (row.canRun) {
            fetch(`http://localhost:8082/jobs/${row.id}/start`, {  
                method: 'POST',
            })
            .then((response) => {
                if (response.ok) {
                    //alert(`Job "${row.id}" started successfully.`);
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
            fetch(`http://localhost:8082/jobs/${row.id}/cancel`, {  
                method: 'DELETE', 
            })
            .then((response) => {
                if (response.ok) {
                    //alert(`Job "${row.id}" cancelled successfully.`);
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
        { field: "id", headerName: "ID", minWidth: 5, type: "number" },
        { field: "algorithm", headerName: "Algorithm", minWidth: 20 },
        { field: "dataset", headerName: "Dataset ID", minWidth: 10, type: "number" },
        { field: "status", headerName: "Status", minWidth: 40 },    
        { field: "maxEntries", headerName: "MAX REC", minWidth: 50, type: "number" },
        { field: "skipEntries", headerName: "SKIP REC", minWidth: 50, type: "number" },
        { field: "maxLHS", headerName: "MAX LHS", minWidth: 10, type: "number" },
        { field: "output", headerName: "Output", minWidth: 10 },
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
       {
        field: "actions2",
        type: "actions",
        minWidth: 180,
        renderCell: (params) => (
            <>
                <Link to={"/jobs/${params.id}"}>
                    <Tooltip title="Edit Job">
                    <IconButton variant="contained" size="small" sx={{ mr: 1 }}  aria-label="edit">
                        <EditIcon />
                    </IconButton>
                    </Tooltip>
                </Link>
                <Link to={"/jobs"}>
                    <Tooltip title="???">
                    <IconButton variant="outlined" size="small" sx={{ mr: 1 }} aria-label="create job">
                        <FindInPageIcon />
                    </IconButton>
                    </Tooltip>
                </Link>
                <Tooltip title={params.row.canDelete ? "??? " : "???"}>
                <IconButton 
                    aria-label="download"
                    variant="outlined" 
                    size="small"
                    sx={{ mr: 1 }}
                    disabled={!params.row.canDelete}    // TODO zmenit na canDownload
                    onClick={() => handleDownload(params.row)}
                >
                    <DownloadIcon />
                </IconButton>
                </Tooltip>
                <Tooltip title={params.row.canDelete ? "Delete Job" : "Delete Disabled"}>
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
                </Tooltip>
            </>
        )
       }
    ];

    const handleDelete = (row) => {
        if (row.canDelete) {
            if (window.confirm(`Are you sure you want to delete job "${row.id}"? This action cannot be undone.`)) {
                fetch(`http://localhost:8082/jobs/${row.id}/delete`, {
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
        <Typography fontWeight="medium" sx={{ flex: 1, mx: 0.5 }}>
            Toolbar
        </Typography>
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
