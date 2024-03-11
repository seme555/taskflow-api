# OpenRouter Credit Monitoring Utility

A comprehensive utility for monitoring OpenRouter API credit usage and sending alerts when credits are low.

## Features

- **Real-time Credit Monitoring**: Fetch current usage from OpenRouter API
- **Configurable Alerts**: Set warning and critical thresholds
- **Historical Tracking**: Store credit usage history in Redis
- **Alert Cooldown**: Prevent spam alerts with configurable cooldown periods
- **Multiple Commands**: Check, monitor, and view history
- **Redis Integration**: Store alerts and history for application integration

## Files

- `monitor_openrouter_credits.py` - Main monitoring utility with full Redis integration
- `test_openrouter_credits.py` - Simple test script without Redis dependency

## Configuration

The utility uses environment variables for configuration:

```bash
# Required
OPENROUTER_API_KEY=your_openrouter_api_key

# Optional (with defaults)
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
REDIS_URL=redis://localhost:6379/0
CREDIT_WARNING_THRESHOLD=10.0
CREDIT_CRITICAL_THRESHOLD=5.0
CREDIT_CHECK_INTERVAL_MINUTES=30
CREDIT_ALERT_COOLDOWN_HOURS=1
```

## Usage

### Basic Credit Check

```bash
# Single credit check
python3 utils/monitor_openrouter_credits.py check

# Or just run without arguments
python3 utils/monitor_openrouter_credits.py
```

### Continuous Monitoring

```bash
# Start continuous monitoring (checks every 30 minutes by default)
python3 utils/monitor_openrouter_credits.py monitor
```

### View Credit History

```bash
# View last 24 hours of credit history
python3 utils/monitor_openrouter_credits.py history

# View last 48 hours
python3 utils/monitor_openrouter_credits.py history 48
```

### Test API Connection

```bash
# Test OpenRouter API connection without Redis
python3 utils/test_openrouter_credits.py
```

## OpenRouter API Response

The utility works with the OpenRouter `/auth/key` endpoint which returns:

```json
{
  "data": {
    "label": "sk-or-v1-...",
    "limit": null,
    "usage": 292.0178864635,
    "is_provisioning_key": false,
    "limit_remaining": null,
    "is_free_tier": false,
    "rate_limit": {
      "requests": 230,
      "interval": "10s"
    }
  }
}
```

**Note**: The `usage` field represents total usage amount (not remaining balance). The utility currently uses this as a proxy for monitoring, but in a production environment, you would need to track the original balance separately to calculate remaining credits.

## Redis Data Structure

The utility stores data in Redis with the following keys:

### Current Status
- `openrouter:credits:current` - Hash with current credit information
- `openrouter:monitoring:status` - Hash with monitoring status

### Historical Data
- `openrouter:credits:history:{timestamp}` - Individual history entries
- `openrouter:credits:timeline` - Sorted set for time-based queries

### Alerts
- `openrouter:alerts:{timestamp}` - Individual alert records
- `openrouter:alerts:list` - List of recent alerts
- `openrouter:alerts:last:{type}` - Cooldown tracking

### Real-time Updates
- `openrouter-alerts` - Redis pub/sub channel for real-time alerts

## Alert Types

- **Warning**: Triggered when usage exceeds warning threshold
- **Critical**: Triggered when usage exceeds critical threshold

Alerts include cooldown periods to prevent spam and are stored in Redis for application integration.

## Integration with Main Application

The utility is designed to integrate with the main FastAPI application:

