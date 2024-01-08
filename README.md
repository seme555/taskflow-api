# TaskFlow API

**A production-ready, distributed task processing system built with FastAPI, Python, and Redis and a React/Tailwind frontend**

This project provides a robust framework for handling asynchronous tasks, featuring a sophisticated queueing system, circuit breaker protection, and a comprehensive real-time monitoring dashboard.

[![Python](https://img.shields.io/badge/Python-3.11-green.svg)](https://www.python.org/downloads/release/python-311/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110-green.svg)](https://fastapi.tiangolo.com/)
[![Celery](https://img.shields.io/badge/Celery-5.3-green.svg)](https://docs.celeryq.dev/en/stable/)
[![Redis](https://img.shields.io/badge/Redis-7.2-green.svg)](https://redis.io/)
[![React](https://img.shields.io/badge/React-18-blue.svg)](https://reactjs.org/)
[![ReactFlow](https://img.shields.io/badge/ReactFlow-12-blue.svg)](https://reactflow.dev/)
[![TailwindCSS](https://img.shields.io/badge/TailwindCSS-3-blue.svg)](https://tailwindcss.com/)
[![Docker](https://img.shields.io/badge/Docker-24-red.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-red.svg)](LICENSE)

## 1. Overview

TaskFlow API is designed to solve the common problem of managing long-running, resource-intensive tasks in a web application without blocking the main request-response cycle. It provides a scalable and resilient architecture for processing tasks in the background, complete with error handling, automatic retries, and detailed monitoring.

To demonstrate the system's capabilities with realistic resource-intensive scenarios, we simulate two demanding remote API tasks using OpenRouter calls to large language models: **text summarization** of articles and **extraction of articles from newspaper PDF files**. These tasks represent typical real-world use cases where external API calls can be slow, expensive, and subject to rate limits, making them perfect examples for showcasing the system's distributed task processing, circuit breaker protection, and rate limiting capabilities.

### Key Features

- **Asynchronous Task Processing**: Distributed task queue using a custom Redis-based solution.
- **Decoupled Architecture**: API, queuing, and execution layers are fully independent.
- **Circuit Breaker Protection**: Built-in circuit breaker for external API calls to prevent cascading failures.
- **Token Bucket Rate Limiting**: Distributed rate limiting mechanism to prevent remote resource exhaustion from parallel independent workers, ensuring compliance with external API limits.
- **Advanced Retry Logic**: Intelligent retry scheduling with exponential backoff and dedicated retry queues.
- **Real-time Monitoring**: A comprehensive frontend dashboard with live updates via Server-Sent Events (SSE).
- **Production Ready**: Docker-based deployment with proper logging, health checks, and resource management.
- **Horizontal Scalability**: Easily scale worker capacity to meet demand.

## 2. Screenshots

Here are some screenshots of the application in action:

**Task State Flow Overview**
![Task State Flow Overview](screenshots/task_state_flow_overview.png)

**Queues, Workers, and Circuitbreaker**
![Queues, Workers, and Circuitbreaker](screenshots/queues__workers_circtuibreaker.png)

**Tasks History**
![Tasks History](screenshots/tasks_history.png)

**Task Detail**
![Task Detail](screenshots/task_detail.png)

**APIs**
![APIs](screenshots/apis.png)

## 3. Table of Contents

- [TaskFlow API](#taskflow-api)
  - [1. Overview](#1-overview)
    - [Key Features](#key-features)
  - [2. Screenshots](#2-screenshots)
  - [3. Table of Contents](#3-table-of-contents)
  - [4. Architecture](#4-architecture)
    - [System Architecture Diagram](#system-architecture-diagram)
    - [Tech Stack](#tech-stack)
    - [Key Components](#key-components)
    - [Data Flow \& Task Lifecycle](#data-flow--task-lifecycle)
  - [5. Prerequisites](#5-prerequisites)
  - [6. Quick Start](#6-quick-start)
  - [7. Usage](#7-usage)
  - [8. API Reference](#8-api-reference)
    - [Health Checks](#health-checks)
    - [Task Creation (Application-Specific)](#task-creation-application-specific)
    - [Generic Task Management](#generic-task-management)
    - [Queue Monitoring \& Management](#queue-monitoring--management)
    - [Worker Management](#worker-management)
    - [OpenRouter Service Management](#openrouter-service-management)
  - [9. Redis Data Structures](#9-redis-data-structures)
    - [Task Queues](#task-queues)
    - [Task Metadata](#task-metadata)
    - [Monitoring \& Metrics](#monitoring--metrics)
    - [Rate Limiting \& External API Management](#rate-limiting--external-api-management)
    - [Circuit Breaker \& Worker Management](#circuit-breaker--worker-management)
  - [10. Development](#10-development)
    - [Running Locally](#running-locally)
    - [System Reset](#system-reset)
  - [11. Distributed Rate Limiting](#11-distributed-rate-limiting)
    - [Overview](#overview)
    - [Key Features](#key-features-1)
    - [How It Works](#how-it-works)
    - [Configuration](#configuration)
    - [Usage Examples](#usage-examples)
      - [Automatic Rate Limiting (Recommended)](#automatic-rate-limiting-recommended)
      - [Manual Rate Limiting](#manual-rate-limiting)
      - [Monitoring Rate Limit Status](#monitoring-rate-limit-status)
    - [Testing the Rate Limiter](#testing-the-rate-limiter)
    - [Rate Limit Data Structures](#rate-limit-data-structures)
      - [Configuration](#configuration-1)
      - [Token Bucket State](#token-bucket-state)
    - [Benefits](#benefits)
    - [Monitoring and Observability](#monitoring-and-observability)
  - [12. Performance Management](#12-performance-management)
    - [OpenRouter State Management Optimization](#openrouter-state-management-optimization)
    - [Rate Limiting Performance Tuning](#rate-limiting-performance-tuning)
  - [13. Collaboration](#13-collaboration)
    - [How to Contribute](#how-to-contribute)
    - [Reporting Issues](#reporting-issues)
  - [14. Troubleshooting](#14-troubleshooting)

## 4. Architecture

The system is composed of several key services, each running in its own Docker container and orchestrated by Docker Compose.

### System Architecture Diagram

```
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”     â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”     â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚                 â”‚     â”‚                 â”‚     â”‚                 â”‚
â”‚  Client Request â”‚â”€â”€â”€â”€â–¶â”‚   FastAPI App   â”‚â”€â”€â”€â”€â–¶â”‚  Redis Storage  â”‚
â”‚ (e.g., Frontend)â”‚     â”‚   (REST API)    â”‚     â”‚ (Task & Metrics)â”‚
â”‚                 â”‚     â”‚                 â”‚     â”‚                 â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜     â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜     â””â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                                                          â”‚
                        â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”               â”‚
                        â”‚                 â”‚               â”‚
                        â”‚  Custom Redis   â”‚â—€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                        â”‚     Queues      â”‚
                        â”‚                 â”‚
