// frontend/src/components/dashboard/TaskFlowGraph.tsx
import React, { useCallback, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  ReactFlow,
  Node,
  Edge,
  addEdge,
  Connection,
  useNodesState,
  useEdgesState,
  Controls,
  Background,
  BackgroundVariant,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { QueueStatus } from '../../lib/api';
import {
  ActiveNode,
  CompletedNode,
  PrimaryQueueNode,
  ScheduledQueueNode,
  RetryNode,
  FailedNode,
  DeadLetterNode,
} from './CustomNodes';

// Define nodeTypes object for React Flow
const nodeTypes = {
  active: ActiveNode,
  completed: CompletedNode,
  primaryQueue: PrimaryQueueNode,
  scheduledQueue: ScheduledQueueNode,
  retry: RetryNode,
  failed: FailedNode,
  deadLetter: DeadLetterNode,
};

interface TaskFlowGraphProps {
  queueStatus: QueueStatus | null;
}

const TaskFlowGraph: React.FC<TaskFlowGraphProps> = ({ queueStatus }) => {
  const navigate = useNavigate();

  // Function to get filter parameters for navigation
  const getFilterParams = (nodeId: string): string => {
    const filterMap: Record<string, string> = {
      completed: 'status=COMPLETED',
      failed: 'status=FAILED',
      active: 'status=ACTIVE',
      primaryQueue: 'queue=primary',
      scheduledQueue: 'queue=scheduled',
      retryQueue: 'queue=retry',
      deadLetter: 'status=DLQ',
    };
    return filterMap[nodeId] || '';
  };

  // Function to get node count for determining if clickable
  const getNodeCount = useCallback((nodeId: string): number => {
    if (!queueStatus) return 0;

    const countMap: Record<string, number> = {
      completed: queueStatus.states.COMPLETED || 0,
      failed: queueStatus.states.FAILED || 0,
      active: queueStatus.states.ACTIVE || 0,
      primaryQueue: queueStatus.queues.primary || 0,
      scheduledQueue: queueStatus.queues.scheduled || 0,
      retryQueue: queueStatus.queues.retry || 0,
      deadLetter: queueStatus.queues.dlq || 0,
    };
    return countMap[nodeId] || 0;
  }, [queueStatus]);

  // Handle node click for navigation
  const handleNodeClick = useCallback((_event: React.MouseEvent, node: Node) => {
    const count = getNodeCount(node.id);
    if (count > 0) {
      const filterParams = getFilterParams(node.id);
      if (filterParams) {
        navigate(`/tasks-history?${filterParams}`);
      }
    }
  }, [navigate, getNodeCount]);

  // Define the initial nodes based on the task flow - updated layout with custom nodes
  const initialNodes: Node[] = useMemo(() => {
    const nodes: Node[] = [
      {
        id: 'submit',
        type: 'input',
        position: { x: 330, y: 50 },
        data: {
          label: (
            <div className="text-center text-gray-800">
              <div className="font-semibold">Task Submit</div>
            </div>
          ),
        },
        className: 'bg-blue-100 border-2 border-blue-600 rounded-lg p-3 w-[140px] h-[80px] flex flex-col justify-center text-center text-gray-900',
      },
      {
        id: 'primaryQueue',
        type: 'primaryQueue',
        position: { x: 330, y: 150 },
        data: {
          label: 'Primary',
          count: queueStatus?.queues.primary || 0,
        },
      },
      {
        id: 'active',
        type: 'active',
        position: { x: 330, y: 250 },
        data: {
          label: 'Active',
          count: queueStatus?.states.ACTIVE || 0,
        },
      },
      {
        id: 'completed',
        type: 'completed',
        position: { x: 100, y: 250 },
        data: {
