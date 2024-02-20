// Mirrors src/api/schemas.py

export enum TaskState {
  PENDING = "PENDING",
  ACTIVE = "ACTIVE",
  COMPLETED = "COMPLETED",
  FAILED = "FAILED",
  SCHEDULED = "SCHEDULED",
  DLQ = "DLQ",
}

export enum TaskType {
  SUMMARIZE = "summarize",
  PDFXTRACT = "pdfxtract",
}

export enum QueueName {
  PRIMARY = "primary",
  RETRY = "retry",
  SCHEDULED = "scheduled",
  DLQ = "dlq",
}

export interface StateHistoryEntry {
  state: TaskState;
  timestamp: string;
  [key: string]: unknown;
}

export interface TaskDetail {
  task_id: string;
  state: TaskState;
  content: string;
  retry_count: number;
  max_retries: number;
  last_error: string | null;
  error_type: string | null;
  retry_after: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
  result: string | null;
  task_type: TaskType | null;
  error_history: Record<string, unknown>[];
  state_history: StateHistoryEntry[];
}

export interface TaskSummary {
  task_id: string;
  state: TaskState;
  retry_count: number;
  max_retries: number;
  last_error: string | null;
  error_type: string | null;
  retry_after: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
  task_type: TaskType | null;
  content_length: number | null;
  has_result: boolean;
  error_history: Record<string, unknown>[];
  state_history: StateHistoryEntry[];
}

export interface TaskListResponse {
  tasks: TaskDetail[];
  page: number;
  page_size: number;
  total_items: number;
  total_pages: number;
  status: TaskState | null;
}

export interface TaskSummaryListResponse {
  tasks: TaskSummary[];
  page: number;
  page_size: number;
  total_items: number;
  total_pages: number;
  status: TaskState | null;
}

export interface TaskResponse {
  task_id: string;
  state: TaskState;
}

export interface TaskDeleteResponse {
  task_id: string;
  message: string;
}
