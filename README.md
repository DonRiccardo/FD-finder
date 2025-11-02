# FD-Finder
FD-Finder is a web application designed to help users identify functional dependencies within their datasets. It provides an intuitive interface for uploading data, analyzing it for functional dependencies, and visualizing the results.

## Features
- Upload datasets in various formats (CSV, JSON)
- Analyze datasets to find functional dependencies
- Show results and statistics of Job runs
- Download found FDs

## Setup Instructions
1. Read documentation "docs/FD-Finder-documentation.pdf" (contains user guide and technical details)
2. Clone the repository
3. Navigate to the project directory
4. Run `docker compose up --build` to start the application
5. Access the application at http://localhost:5173

## Contents

These files and directotries are the main components of the FD-Finder project:

- `backend/`: Contains the backend services including the demo algorithm service.
- `frontend/`: Contains the frontend application built with React.
- `docs/`: Contains documentation file including user guides and technical details.
- `api-docs/`: Contains API documentation for the backend services.
- `specification/`: Contains project specification and requirements.
- `docker-compose.yml`: Docker Compose file to set up and run the application.
