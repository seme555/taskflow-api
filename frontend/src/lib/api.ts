import axios from "axios";

const API_BASE_URL = "/api/v1";

export interface QueueDepths {
  primary: number;
  retry: number;
  scheduled: number;
  dlq: number;
  [key: string]: number;
}

export interface StateCounts {
  PENDING: number;
  ACTIVE: number;
  COMPLETED: number;
  FAILED: number;
  SCHEDULED: number;
  DLQ: number;
  [key: string]: number;
}

export interface QueueStatus {
  queues: QueueDepths;
  states: StateCounts;
  retry_ratio: number;
}

export interface SSEMessage {
  type: "initial_status" | "queue_update" | "heartbeat" | "error" | "fatal_error";
  queue_depths?: QueueDepths;
  state_counts?: StateCounts;
  retry_ratio?: number;
  message?: string;
  timestamp?: number;
}

export interface OpenRouterStatus {
  status: string;
  message: string;
  balance?: number;
  usage_today?: number;
  usage_month?: number;
  last_check?: string;
  error_details?: string;
  consecutive_failures?: number;
  circuit_breaker_open?: boolean;
  cache_hit?: boolean;
}

export interface CircuitBreakerStatus {
  state: string;
  success_count?: number;
  fail_count?: number;
  note?: string;
}

export interface WorkerDetail {
  worker_id: string;
  worker_name?: string;
  status: string;
  last_heartbeat?: string | number;
  timestamp?: string | number;
  heartbeat_age_seconds?: number;
  error?: string;
  circuit_breaker: CircuitBreakerStatus;
}

export interface WorkerStatus {
  overall_status: string;
  total_workers: number;
  healthy_workers: number;
  stale_workers: number;
  circuit_breaker_states?: Record<string, number>;
  worker_details: WorkerDetail[];
  timestamp: string;
  error?: string;
}

class ApiService {
  async getOpenRouterStatus(): Promise<OpenRouterStatus> {
    const response = await axios.get<OpenRouterStatus>(`${API_BASE_URL}/openrouter/status`);
    return response.data;
  }

  async getWorkerStatus(): Promise<WorkerStatus> {
    const response = await axios.get<WorkerStatus>(`${API_BASE_URL}/workers/`);
    return response.data;
  }

  createSSEConnection(
    onMessage: (data: SSEMessage) => void,
    onError: (error: Event) => void
  ): EventSource {
    const eventSource = new EventSource(`${API_BASE_URL}/queues/status/stream`);

    eventSource.onmessage = (event: MessageEvent) => {
      try {
        const data = JSON.parse(event.data) as SSEMessage;
        onMessage(data);
      } catch (error) {
        console.error("Failed to parse SSE message:", error);
      }
    };

    eventSource.onerror = (error: Event) => {
      onError(error);
    };

    return eventSource;
  }
}

export const apiService = new ApiService();
