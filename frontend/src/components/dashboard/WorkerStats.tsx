// frontend/src/components/dashboard/WorkerStats.tsx
import React, { useState, useEffect, useRef } from 'react';
import { apiService, WorkerStatus, WorkerDetail } from '../../lib/api';

interface WorkerStatsProps {
  isConnected: boolean;
}

const WorkerStats: React.FC<WorkerStatsProps> = ({ isConnected }) => {
  const [workerStatus, setWorkerStatus] = useState<WorkerStatus | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [isUpdating, setIsUpdating] = useState<boolean>(false);
  const initialLoadRef = useRef<boolean>(true);

  useEffect(() => {
    const fetchWorkerStats = async () => {
      if (!isConnected) {
        if (initialLoadRef.current) {
          setLoading(false);
        }
        return;
      }

      try {
        // Only show loading spinner on initial load, not on updates
        if (initialLoadRef.current) {
          setLoading(true);
        } else {
          setIsUpdating(true);
        }
        setError(null);
        
        const response = await apiService.getWorkerStatus();
        setWorkerStatus(response);
        
        if (initialLoadRef.current) {
          initialLoadRef.current = false;
        }
      } catch (err) {
        console.error('Failed to fetch worker stats:', err);
        setError('Failed to load worker statistics');
        setWorkerStatus(null);
      } finally {
        setLoading(false);
        setIsUpdating(false);
      }
    };

    fetchWorkerStats();
    
    // Refresh worker stats every 30 seconds if connected
    const interval = isConnected ? setInterval(fetchWorkerStats, 30000) : null;
    
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [isConnected]);

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case 'healthy':
        return 'bg-green-100 text-green-800';
      case 'stale':
        return 'bg-yellow-100 text-yellow-800';
      case 'no_heartbeat':
      case 'error':
      case 'unhealthy':
        return 'bg-red-100 text-red-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  const formatLastHeartbeat = (worker: WorkerDetail) => {
    // Handle both timestamp (from broadcast) and last_heartbeat (from fallback)
    const heartbeat = worker.last_heartbeat || worker.timestamp;
    if (!heartbeat) return 'Never';
    try {
      // Try parsing as number first (Unix timestamp), then as ISO string
      const timestamp = isNaN(Number(heartbeat)) ? heartbeat : Number(heartbeat) * 1000;
      const date = new Date(timestamp);
      return date.toLocaleTimeString();
    } catch {
      return 'Unknown';
    }
  };


  const getCircuitBreakerColor = (state: string) => {
    switch (state.toLowerCase()) {
      case 'closed':
        return 'bg-green-100 text-green-800';
      case 'half-open':
        return 'bg-yellow-100 text-yellow-800';
      case 'open':
        return 'bg-red-100 text-red-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  if (!isConnected) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">
          Worker Statistics
        </h3>
        <div className="text-center py-8">
          <div className="text-gray-500">
            <svg className="mx-auto h-12 w-12 text-gray-400 mb-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.172 16.172a4 4 0 015.656 0M9 12h6m-6-4h6m2 5.291A7.962 7.962 0 0112 15c-2.34 0-4.47-.881-6.08-2.33" />
            </svg>
            <p className="text-sm">Connection required to display worker statistics</p>
          </div>
        </div>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">
          Worker Statistics
