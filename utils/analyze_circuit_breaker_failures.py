#!/usr/bin/env python3
"""
Script to analyze circuit breaker failures and extract root cause information.
"""

import redis
import json
import os
from collections import Counter, defaultdict

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


def analyze_error_patterns():
    """Analyze error patterns from DLQ tasks and task history."""
    r = redis.from_url(REDIS_URL, decode_responses=True)

    print("ðŸ” Analyzing Circuit Breaker Failure Root Causes")
    print("=" * 60)

    # Get all DLQ task IDs
    dlq_task_ids = r.lrange("dlq:tasks", 0, -1)
    print(f"Found {len(dlq_task_ids)} tasks in DLQ")

    error_analysis = {
        "http_errors": Counter(),
        "error_types": Counter(),
        "timeline": [],
        "detailed_errors": [],
    }

    # Analyze each DLQ task
    for task_id in dlq_task_ids:
        task_data = r.hgetall(f"task:{task_id}")
        if not task_data:
            continue

        print(f"\nðŸ“‹ Task: {task_id}")
        print(f"   State: {task_data.get('state', 'unknown')}")
        print(f"   Last Error: {task_data.get('last_error', 'none')}")
        print(f"   Error Type: {task_data.get('error_type', 'unknown')}")
        print(f"   Retry Count: {task_data.get('retry_count', 0)}")

        # Parse error history
        error_history = []
        if task_data.get("error_history"):
            try:
                error_history = json.loads(task_data["error_history"])
            except (json.JSONDecodeError, TypeError):
                pass

        if error_history:
            print(f"   ðŸ“œ Error History ({len(error_history)} entries):")
            for i, error_entry in enumerate(error_history, 1):
                error_msg = error_entry.get("error", "unknown")
                timestamp = error_entry.get("timestamp", "unknown")
                error_type = error_entry.get("error_type", "unknown")
                retry_count = error_entry.get("retry_count", 0)

                print(f"      {i}. [{timestamp}] Retry {retry_count}: {error_msg}")

                # Extract HTTP status codes
                if "HTTP" in error_msg and any(
                    code in error_msg
                    for code in ["429", "502", "503", "500", "401", "402", "403", "404"]
                ):
                    for code in [
                        "429",
                        "502",
                        "503",
                        "500",
                        "401",
                        "402",
                        "403",
                        "404",
                    ]:
                        if code in error_msg:
                            error_analysis["http_errors"][code] += 1
                            break

                # Count error types
                error_analysis["error_types"][error_type] += 1

                # Add to timeline
                error_analysis["timeline"].append(
                    {
                        "task_id": task_id,
                        "timestamp": timestamp,
                        "error": error_msg,
                        "error_type": error_type,
                        "retry_count": retry_count,
                    }
                )

                # Add detailed error for analysis
                error_analysis["detailed_errors"].append(
                    {
                        "task_id": task_id,
                        "error": error_msg,
                        "error_type": error_type,
                        "timestamp": timestamp,
                    }
                )

    # Analyze all task states to find patterns
    print("\nðŸ” Scanning all tasks for error patterns...")
    error_patterns = defaultdict(int)
    circuit_breaker_triggers = []

    for key in r.scan_iter("task:*"):
        task_data = r.hgetall(key)
        if not task_data:
            continue

        # Check for error history in any task
        if task_data.get("error_history"):
            try:
                error_history = json.loads(task_data["error_history"])
                for error_entry in error_history:
                    error_msg = error_entry.get("error", "")

                    # Look for circuit breaker related errors
                    if "circuit breaker" in error_msg.lower():
                        circuit_breaker_triggers.append(
                            {
                                "task_id": key.split(":", 1)[1],
                                "error": error_msg,
                                "timestamp": error_entry.get("timestamp", "unknown"),
                            }
                        )

                    # Pattern analysis
                    if "429" in error_msg:
                        error_patterns["rate_limit"] += 1
                    elif "502" in error_msg:
                        error_patterns["bad_gateway"] += 1
                    elif "503" in error_msg:
                        error_patterns["service_unavailable"] += 1
                    elif "timeout" in error_msg.lower():
                        error_patterns["timeout"] += 1
                    elif "insufficient credits" in error_msg.lower():
                        error_patterns["insufficient_credits"] += 1
                    elif "connection" in error_msg.lower():
                        error_patterns["connection_error"] += 1

            except (json.JSONDecodeError, TypeError):
                pass

    # Print analysis results
    print("\nðŸ“Š ERROR ANALYSIS SUMMARY")
    print("=" * 40)

    print("\nðŸš¨ HTTP Error Codes:")
    for code, count in error_analysis["http_errors"].most_common():
        code_meanings = {
            "429": "Too Many Requests (Rate Limited)",
            "502": "Bad Gateway",
            "503": "Service Unavailable",
            "500": "Internal Server Error",
            "401": "Unauthorized",
            "402": "Payment Required (Insufficient Credits)",
            "403": "Forbidden",
            "404": "Not Found",
        }
        meaning = code_meanings.get(code, "Unknown")
        print(f"   HTTP {code}: {count} occurrences - {meaning}")

    print("\nðŸ·ï¸  Error Types:")
    for error_type, count in error_analysis["error_types"].most_common():
        print(f"   {error_type}: {count} occurrences")

    print("\nðŸ”„ Error Patterns:")
    for pattern, count in error_patterns.items():
        print(f"   {pattern}: {count} occurrences")

    if circuit_breaker_triggers:
        print(f"\nâš¡ Circuit Breaker Triggers ({len(circuit_breaker_triggers)}):")
        for trigger in circuit_breaker_triggers:
            print(
                f"   Task {trigger['task_id']}: {trigger['error']} at {trigger['timestamp']}"
            )

    # Determine most likely root cause
    print("\nðŸŽ¯ ROOT CAUSE ANALYSIS")
    print("=" * 30)

    if error_analysis["http_errors"]:
        most_common_error = error_analysis["http_errors"].most_common(1)[0]
        error_code, count = most_common_error

        root_causes = {
            "429": "ðŸš« RATE LIMITING: OpenRouter API is rate limiting requests. This is the most common cause of circuit breaker failures.",
            "502": "ðŸŒ GATEWAY ISSUES: Bad gateway errors indicate network/proxy issues between workers and OpenRouter.",
            "503": "âš ï¸  SERVICE UNAVAILABLE: OpenRouter service is experiencing high load or temporary outages.",
            "402": "ðŸ’³ INSUFFICIENT CREDITS: OpenRouter account has insufficient credits to process requests.",
            "500": "ðŸ”¥ SERVER ERRORS: OpenRouter is experiencing internal server errors.",
            "401": "ðŸ” AUTHENTICATION: Invalid or expired OpenRouter API key.",
            "403": "ðŸš· FORBIDDEN: API key doesn't have permission for the requested operation.",
            "404": "â“ NOT FOUND: Invalid API endpoint or model not found.",
        }

        root_cause = root_causes.get(error_code, f"Unknown HTTP {error_code} errors")
        print("\nMOST LIKELY ROOT CAUSE:")
        print(f"{root_cause}")
        print(f"Occurred {count} times in the analyzed tasks.")

        # Provide recommendations
        recommendations = {
            "429": [
                "â€¢ Implement exponential backoff with longer delays",
                "â€¢ Reduce worker concurrency to stay within rate limits",
                "â€¢ Consider upgrading OpenRouter plan for higher rate limits",
                "â€¢ Add jitter to prevent thundering herd effects",
            ],
            "502": [
                "â€¢ Check network connectivity between workers and OpenRouter",
                "â€¢ Verify DNS resolution for openrouter.ai",
                "â€¢ Consider adding retry logic for gateway errors",
                "â€¢ Check if there are any proxy/firewall issues",
            ],
            "503": [
                "â€¢ Implement longer retry delays for service unavailable errors",
                "â€¢ Monitor OpenRouter status page for service issues",
                "â€¢ Consider implementing fallback mechanisms",
                "â€¢ Add circuit breaker timeout adjustments",
            ],
            "402": [
                "â€¢ Check OpenRouter account balance and add credits",
                "â€¢ Implement credit monitoring and alerts",
                "â€¢ Consider upgrading to a higher tier plan",
                "â€¢ Add proper error handling for insufficient credits",
            ],
            "401": [
                "â€¢ Verify OpenRouter API key is correct and active",
                "â€¢ Check if API key has expired",
                "â€¢ Ensure API key is properly configured in environment variables",
                "â€¢ Test API key with a simple curl request",
            ],
        }

        if error_code in recommendations:
            print("\nðŸ’¡ RECOMMENDATIONS:")
            for rec in recommendations[error_code]:
                print(rec)

    else:
        print("No clear HTTP error pattern found. Check worker logs for more details.")

    return error_analysis


def main():
    try:
        analyze_error_patterns()
        print(
            "\nâœ… Analysis complete. Check the detailed output above for root cause information."
        )
    except Exception as e:
        print(f"âŒ Analysis failed: {e}")


if __name__ == "__main__":
    main()
