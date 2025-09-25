import React, { useEffect, useState } from "react";
import { Button, Box, Stack, IconButton } from "@mui/material";
import {
  DataGrid,
  Toolbar,
  ToolbarButton,
  ColumnsPanelTrigger,
  FilterPanelTrigger,
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
import Typography from '@mui/material/Typography';
import Badge from '@mui/material/Badge';


export default function DatasetsAll() {

    const [pageSize, setPageSize] = useState(20);
    const [rows, setRows] = useState([]);

    const fetchDatasets = React.useCallback(async () => {
        try {
            fetch("http://localhost:8081/datasets")
            .then((response) => response.json())
            .then((data) => {

                const datasets = data._embedded?.datasetList || [];

                const formatted = datasets.map((dataset) => ({
                    ...dataset,
                    canDelete: Boolean(dataset._links?.delete),
                    createdAt: dataset.createdAt ? new Date(dataset.createdAt.replace(/(\.\d{3})\d+/, "$1")) : null,
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
        fetchDatasets();
    }, [fetchDatasets]);
    

    const columns = [
        { field: "id", headerName: "ID", minWidth: 5, type: "number" },
        { field: "name", headerName: "Name", minWidth: 200 },
        { field: "fileFormat", headerName: "Format", minWidth: 10},
        { field: "description", headerName: "Description", minWidth: 200 },    
        { field: "numEntries", headerName: "#REC", minWidth: 50, type: "number" },
        { field: "numAttributes", headerName: "#ATT", minWidth: 50, type: "number" },
        { field: "delim", headerName: "Delim", minWidth: 10, align: "center", headerAlign: "center" },
        { field: "header", headerName: "Header", minWidth: 10, type: "boolean" },
        { field: "createdAt", 
            headerName: "Created At", 
            minWidth: 150,
            type: "dateTime"
        },
        /*
        { field: "updatedAt", 
            headerName: "Updated At", 
            width: 200,
            type: "dateTime",
            valueGetter: ({ value }) => value && new Date(value),
        },
        */
       {
        field: "actions",
        headerName: "Actions",
        minWidth: 180,
        renderCell: (params) => (
            <>
                <Link to={"/datasets/${params.id}"}>
                    <Tooltip title="Edit Dataset">
                    <IconButton variant="contained" size="small" sx={{ mr: 1 }}  aria-label="edit">
                        <EditIcon />
                    </IconButton>
                    </Tooltip>
                </Link>
                <Link to={"/jobs"}>
                    <Tooltip title="Create Job with this Dataset">
                    <IconButton variant="outlined" size="small" sx={{ mr: 1 }} aria-label="create job">
                        <FindInPageIcon />
                    </IconButton>
                    </Tooltip>
                </Link>
                <Tooltip title={params.row.canDelete ? "Download Dataset" : "Download Disabled"}>
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
                <Tooltip title={params.row.canDelete ? "Delete Dataset" : "Delete Disabled"}>
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
            if (window.confirm(`Are you sure you want to delete dataset "${row.name}"? This action cannot be undone.`)) {
                fetch(`http://localhost:8081/datasets/${row.id}/delete`, {
                    method: 'DELETE',
                })
                .then((response) => {
                    if (response.ok) {
                        alert(`Dataset "${row.name}" deleted successfully.`);
                        fetchDatasets(); // Refresh the dataset list
                    } else {
                        alert(`Failed to delete dataset "${row.name}".`);
                    }
                })
                .catch((error) => {
                    console.error("Error deleting dataset:", error);
                    alert(`Error deleting dataset "${row.name}".`);
                });

            }
        }
    }
    const handleDownload = (row) => {
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
        <Link to={"/datasets/upload"}>
            <Button color="secondary" variant="outlined" startIcon={<AddIcon />}>
                Add Dataset
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
