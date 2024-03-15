#!/usr/bin/env python3
"""
PDF Extraction Task Testing Utility

This script tests the pdfxtract functionality by:
1. Uploading the il-manifesto-del-31-dicembre-2023.pdf file
2. Monitoring the task until completion
3. Saving the extracted articles as il-manifesto-del-31-dicembre-2023.json
"""

import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx


class PdfExtractTester:
    """PDF extraction task tester."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self.client = None

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            timeout=60.0
        )  # Longer timeout for PDF processing
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    async def upload_pdf_for_extraction(
        self, pdf_path: Path, issue_date: Optional[str] = None
    ) -> Optional[str]:
        """Upload PDF file and create extraction task."""
        if not pdf_path.exists():
            print(f"âŒ PDF file not found: {pdf_path}")
            return None

        print(f"ðŸ“„ Uploading PDF: {pdf_path.name}")
        print(f"   File size: {pdf_path.stat().st_size / 1024 / 1024:.2f} MB")

        try:
            # Prepare the multipart form data
            files = {"file": (pdf_path.name, pdf_path.open("rb"), "application/pdf")}

            data = {}
            if issue_date:
                data["issue_date"] = issue_date

            # Upload the PDF
            response = await self.client.post(
                f"{self.base_url}/api/v1/tasks/pdfxtract", files=files, data=data
            )

            if response.status_code == 201:
                result = response.json()
                task_id = result.get("task_id")
                print(f"âœ… Task created successfully: {task_id}")
                print(f"   Initial state: {result.get('state')}")
                return task_id
            else:
                print(f"âŒ Failed to create task: {response.status_code}")
                print(f"   Error: {response.text}")
                return None

        except Exception as e:
            print(f"âŒ Error uploading PDF: {e}")
            return None

    async def monitor_task(
        self, task_id: str, max_wait_time: int = 300
    ) -> Optional[dict]:
        """Monitor task until completion or timeout."""
        print(f"â³ Monitoring task {task_id}...")

        start_time = time.time()
        last_state = None

        while time.time() - start_time < max_wait_time:
            try:
                response = await self.client.get(
                    f"{self.base_url}/api/v1/tasks/{task_id}"
                )

                if response.status_code == 200:
                    task_data = response.json()
                    current_state = task_data.get("state")

                    # Print state changes
                    if current_state != last_state:
                        elapsed = time.time() - start_time
                        print(f"   State: {current_state} (after {elapsed:.1f}s)")
                        last_state = current_state

                        # Show retry information if available
                        if current_state == "SCHEDULED":
                            retry_count = task_data.get("retry_count", 0)
                            retry_after = task_data.get("retry_after")
                            print(
                                f"   Retry #{retry_count}, scheduled for: {retry_after}"
                            )

                        # Show error information if available
                        if task_data.get("last_error"):
                            error_type = task_data.get("error_type", "Unknown")
                            print(
                                f"   Last error ({error_type}): {task_data['last_error'][:100]}..."
                            )

                    # Check if task is complete
                    if current_state == "COMPLETED":
                        print(
                            f"âœ… Task completed successfully in {time.time() - start_time:.1f}s"
                        )
                        return task_data
                    elif current_state == "FAILED":
                        print(f"âŒ Task failed after {time.time() - start_time:.1f}s")
