import axios from "axios";
import type { TaskDetail, TaskDeleteResponse, TaskSummaryListResponse } from "@/lib/types";

const TASKS_BASE_URL = "/api/v1/tasks";

export async function fetchTaskSummaries(
  filters: Record<string, unknown>
): Promise<TaskSummaryListResponse> {
  const { status, ...rest } = filters;
  const params: Record<string, unknown> = { ...rest, task_status: status };

  const response = await axios.get<TaskSummaryListResponse>(`${TASKS_BASE_URL}/summaries/`, {
    params,
  });
  return response.data;
}

export async function fetchTaskDetail(taskId: string): Promise<TaskDetail> {
  const response = await axios.get<TaskDetail>(`${TASKS_BASE_URL}/${taskId}`);
  return response.data;
}

export async function deleteTask(taskId: string): Promise<TaskDeleteResponse> {
  const response = await axios.delete<TaskDeleteResponse>(`${TASKS_BASE_URL}/${taskId}`);
  return response.data;
}
