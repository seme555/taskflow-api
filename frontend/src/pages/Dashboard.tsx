// frontend/src/pages/Dashboard.tsx
import React, { useState, useEffect } from 'react';
import { QueueStatus, SSEMessage, OpenRouterStatus, apiService } from '../lib/api';
import TaskFlowGraph from '../components/dashboard/TaskFlowGraph';
import QueueStats from '../components/dashboard/QueueStats';
import WorkerStats from '../components/dashboard/WorkerStats';

const Dashboard: React.FC = () => {
  const [queueStatus, setQueueStatus] = useState<QueueStatus | null>(null);
  const [isConnected, setIsConnected] = useState<boolean>(false);
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);
  const [openRouterStatus, setOpenRouterStatus] = useState<OpenRouterStatus | null>(null);

  // Fetch OpenRouter status on component mount and periodically
  useEffect(() => {
    const fetchOpenRouterStatus = async () => {
      try {
        const status = await apiService.getOpenRouterStatus();
        setOpenRouterStatus(status);
      } catch (error) {
        console.error('Failed to fetch OpenRouter status:', error);
        setOpenRouterStatus({
          status: 'error',
          message: 'Status check failed'
        });
      }
    };

    // Initial fetch
    fetchOpenRouterStatus();

    // Set up periodic refresh (every 5 minutes)
    const interval = setInterval(fetchOpenRouterStatus, 5 * 60 * 1000);

    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    let eventSource: EventSource | null = null;

    const connectSSE = () => {
      try {
        eventSource = apiService.createSSEConnection(
          (data: SSEMessage) => {
            setLastUpdate(new Date());
            
            switch (data.type) {
              case 'initial_status':
                if (data.queue_depths && data.state_counts && data.retry_ratio !== undefined) {
                  setQueueStatus({
                    queues: data.queue_depths,
                    states: data.state_counts,
                    retry_ratio: data.retry_ratio
                  });
                }
                setIsConnected(true);
                break;
                
              case 'queue_update':
                if (data.queue_depths && data.state_counts && data.retry_ratio !== undefined) {
                  setQueueStatus({
                    queues: data.queue_depths,
                    states: data.state_counts,
                    retry_ratio: data.retry_ratio
                  });
                }
                setIsConnected(true);
                break;
                
              case 'heartbeat':
                setLastUpdate(new Date());
                setIsConnected(true);
                break;
                
              case 'error':
                console.error('SSE Error:', data.message);
                break;
                
              case 'fatal_error':
                console.error('SSE Fatal Error:', data.message);
                setIsConnected(false);
                break;
            }
          },
          (error) => {
            console.error('SSE Connection Error:', error);
            setIsConnected(false);
            
            // Attempt to reconnect after 5 seconds
            setTimeout(() => {
              if (eventSource?.readyState === EventSource.CLOSED) {
                connectSSE();
              }
            }, 5000);
          }
        );

        // Handle connection open
        eventSource.onopen = () => {
          setIsConnected(true);
        };

      } catch (error) {
        console.error('Failed to establish SSE connection:', error);
        setIsConnected(false);
      }
    };

    // Initial connection
    connectSSE();

    // Cleanup on unmount
    return () => {
      if (eventSource) {
        eventSource.close();
      }
    };
  }, []);

  const getConnectionStatus = () => {
    if (isConnected) return { text: 'Connected', color: 'bg-green-500' };
    if (lastUpdate) return { text: 'Trying...', color: 'bg-yellow-500' };
    return { text: 'Disconnected', color: 'bg-red-500' };
  };

