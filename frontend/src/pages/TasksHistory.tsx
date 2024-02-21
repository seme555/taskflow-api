import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { fetchTaskSummaries, fetchTaskDetail, deleteTask } from '@/lib/tasks-api';
import { TaskSummary, TaskSummaryListResponse, TaskDetail, TaskState } from '@/lib/types';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from '@/components/ui/dialog';
import { format } from 'date-fns';
import { ChevronDown, ChevronUp } from 'lucide-react';

const TasksHistory: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [tasksResponse, setTasksResponse] = useState<TaskSummaryListResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deleteModalOpen, setDeleteModalOpen] = useState(false);
  const [taskToDelete, setTaskToDelete] = useState<TaskSummary | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [expandedTaskId, setExpandedTaskId] = useState<string | null>(null);
  const [expandedTaskDetail, setExpandedTaskDetail] = useState<TaskDetail | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);

  const [filters, setFilters] = useState({
    task_id: searchParams.get('task_id') || '',
    status: searchParams.get('status') || 'all',
    task_type: searchParams.get('task_type') || 'all',
    page: parseInt(searchParams.get('page') || '1', 10),
  });

  const loadTasks = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const filterParams: Record<string, unknown> = { 
        ...filters, 
        page_size: 10,
        sort_by: 'created_at',
        sort_order: 'desc'
      };
      // Don't send status filter if "all" is selected
      if (filters.status === 'all' || filters.status === '') {
        filterParams.status = undefined;
      }
      // Don't send task_type filter if "all" is selected
      if (filters.task_type === 'all' || filters.task_type === '') {
        filterParams.task_type = undefined;
      }
      // Only send task_id if it has at least 1 character
      if (filters.task_id && filters.task_id.trim().length > 0) {
        filterParams.task_id = filters.task_id.trim();
      } else {
        filterParams.task_id = undefined;
      }
      const data = await fetchTaskSummaries(filterParams);
      setTasksResponse(data);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  }, [filters]);

  useEffect(() => {
    loadTasks();
    setSearchParams(
      Object.fromEntries(
        Object.entries(filters).map(([key, value]) => [key, String(value)])
      )
    );
  }, [loadTasks, filters, setSearchParams]);

  const handleFilterChange = (key: string, value: string | number) => {
    setFilters((prev) => ({ ...prev, [key]: value, page: 1 }));
  };

  const handlePageChange = (newPage: number) => {
    setFilters((prev) => ({ ...prev, page: newPage }));
  };

  // Check if pagination should be shown
  const shouldShowPagination = useMemo(() => {
    return tasksResponse && tasksResponse.total_pages > 1;
  }, [tasksResponse]);

  const renderStateBadge = (state: TaskState) => {
    const colorMap: { [key in TaskState]: string } = {
      [TaskState.PENDING]: 'bg-yellow-500 border-yellow-600',
      [TaskState.ACTIVE]: 'bg-green-500 border-green-600',
      [TaskState.COMPLETED]: 'bg-green-500 border-green-600',
      [TaskState.FAILED]: 'bg-yellow-500 border-yellow-600',
      [TaskState.SCHEDULED]: 'bg-yellow-500 border-yellow-600',
      [TaskState.DLQ]: 'bg-red-500 border-red-600',
    };
    return <Badge className={`${colorMap[state]} text-white border`}>{state}</Badge>;
  };

  const getTaskType = (task: TaskSummary): string => {
    // Return the task_type from the backend, which should handle the fallback logic
    return task.task_type || 'summarize';
  };

  const renderTaskTypeBadge = (taskType: string) => {
    const colorMap: { [key: string]: string } = {
      'summarize': 'bg-blue-500 border-blue-600',
      'pdfxtract': 'bg-purple-500 border-purple-600',
    };
    return <Badge className={`${colorMap[taskType] || 'bg-gray-500 border-gray-600'} text-white border`}>{taskType}</Badge>;
  };

  const calculateDuration = (task: TaskSummary | TaskDetail): string => {
    if (task.completed_at) {
      const duration = (new Date(task.completed_at).getTime() - new Date(task.created_at).getTime()) / 1000;
      return `${duration}s`;
    }
    return 'N/A';
  };

  const handleDeleteClick = (task: TaskSummary) => {
    setTaskToDelete(task);
    setDeleteModalOpen(true);
  };

