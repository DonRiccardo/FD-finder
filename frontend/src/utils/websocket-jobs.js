import SockJS  from "sockjs-client/dist/sockjs";
import { Client } from "@stomp/stompjs";

const jobServiceURL = import.meta.env.VITE_JOBSERVICE_URL;

/**
 * Listen to webSocket and update Job data.
 * @param {*} setRows 
 * @returns 
 */
export function websocketListen(setRows) {

    const socket = new SockJS(jobServiceURL+"/websocket");
    const stompClient = new Client({
        webSocketFactory: () => socket,
        reconnectDelay: 5000,
        onConnect: () => {

            stompClient.subscribe("/topic/jobs", (message) => {
                const obtainedData = JSON.parse(message.body);
                const _links = Object.fromEntries(
                    obtainedData.links?.map(link => [link.rel, { href: link.href }])
                );
                obtainedData._links = _links;
                const jobUpdate = formatJobMetadaata(obtainedData);
                
                setRows((prev) => {
                    if (Array.isArray(prev)){
                        const index = prev.findIndex((row) => row.id === jobUpdate.id);
                        if (index !== -1) {
                            const updated = [...prev];
                            updated[index] = jobUpdate;
                            return updated;
                        } 
                        else {
                            return [...prev, jobUpdate];
                        }
                    }
                    else if (prev && typeof prev === "object"){
                        return {
                            ...prev,
                            [jobUpdate.jobName]: {
                                ...jobUpdate
                            }
                        };
                    }
                    
                });
            });
        }
    });

    stompClient.activate();

    return () => {
        stompClient.deactivate();
    };
}

/**
 * Formate Job data and create new fields.
 * @param {Object} job to be formatted
 * @returns {Object} formatted job
 */
export function formatJobMetadaata(job) {
    
    return {
        ...job,
        canDelete: Boolean(job._links?.delete),
        canCancel: Boolean(job._links?.cancel),
        canRun: Boolean(job._links?.start),
        createdAt: job.createdAt ? new Date(job.createdAt.replace(/(\.\d{3})\d+/, "$1")) : null,
        updatedAt: job.updatedAt ? new Date(job.updatedAt.replace(/(\.\d{3})\d+/, "$1")) : null,
    };
}