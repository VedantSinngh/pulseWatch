#!/bin/sh
# ============================================================
# PulseWatch — OpenSearch initialization script
# Sets up ILM policy, index templates, and initial indices.
# ============================================================
set -e

OS_URL="http://opensearch:9200"
RETENTION_DAYS="${RETENTION_DAYS:-7}"

echo "==> Waiting for OpenSearch to be ready..."
until curl -sf "${OS_URL}/_cluster/health" | grep -qv '"status":"red"'; do
  echo "   OpenSearch not ready yet, retrying in 5s..."
  sleep 5
done
echo "==> OpenSearch is ready."

# ── ILM Policy ──────────────────────────────────────────────
echo "==> Creating ILM policy (retention=${RETENTION_DAYS} days)..."
curl -s -X PUT "${OS_URL}/_ilm/policy/pulsewatch-raw-policy" \
  -H "Content-Type: application/json" \
  -d "{
    \"policy\": {
      \"phases\": {
        \"hot\": {
          \"min_age\": \"0ms\",
          \"actions\": {
            \"rollover\": {
              \"max_age\": \"1d\",
              \"max_size\": \"5gb\"
            }
          }
        },
        \"delete\": {
          \"min_age\": \"${RETENTION_DAYS}d\",
          \"actions\": {
            \"delete\": {}
          }
        }
      }
    }
  }" && echo ""

# Anomalies kept longer (30 days)
curl -s -X PUT "${OS_URL}/_ilm/policy/pulsewatch-anomalies-policy" \
  -H "Content-Type: application/json" \
  -d '{
    "policy": {
      "phases": {
        "hot": {
          "min_age": "0ms",
          "actions": {
            "rollover": {
              "max_age": "7d"
            }
          }
        },
        "delete": {
          "min_age": "30d",
          "actions": {
            "delete": {}
          }
        }
      }
    }
  }' && echo ""

# ── Index Templates ──────────────────────────────────────────
echo "==> Creating index template for events-raw..."
curl -s -X PUT "${OS_URL}/_index_template/events-raw-template" \
  -H "Content-Type: application/json" \
  -d '{
    "index_patterns": ["events-raw*"],
    "template": {
      "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "index.lifecycle.name": "pulsewatch-raw-policy",
        "index.lifecycle.rollover_alias": "events-raw"
      },
      "mappings": {
        "properties": {
          "sensor_id":        { "type": "keyword" },
          "timestamp":        { "type": "date" },
          "processed_at":     { "type": "date" },
          "temperature":      { "type": "double" },
          "vibration":        { "type": "double" },
          "pressure":         { "type": "double" },
          "z_temperature":    { "type": "double" },
          "z_vibration":      { "type": "double" },
          "z_pressure":       { "type": "double" },
          "cusum_temp_pos":   { "type": "double" },
          "cusum_temp_neg":   { "type": "double" },
          "cusum_vib_pos":    { "type": "double" },
          "cusum_vib_neg":    { "type": "double" },
          "cusum_pres_pos":   { "type": "double" },
          "cusum_pres_neg":   { "type": "double" },
          "if_score":         { "type": "double" },
          "is_anomaly":       { "type": "boolean" },
          "anomaly_types":    { "type": "keyword" },
          "anomaly_injected": { "type": "keyword" },
          "severity":         { "type": "keyword" }
        }
      }
    }
  }' && echo ""

echo "==> Creating index template for events-anomalies..."
curl -s -X PUT "${OS_URL}/_index_template/events-anomalies-template" \
  -H "Content-Type: application/json" \
  -d '{
    "index_patterns": ["events-anomalies*"],
    "template": {
      "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "index.lifecycle.name": "pulsewatch-anomalies-policy",
        "index.lifecycle.rollover_alias": "events-anomalies"
      },
      "mappings": {
        "properties": {
          "sensor_id":        { "type": "keyword" },
          "timestamp":        { "type": "date" },
          "processed_at":     { "type": "date" },
          "temperature":      { "type": "double" },
          "vibration":        { "type": "double" },
          "pressure":         { "type": "double" },
          "anomaly_types":    { "type": "keyword" },
          "severity":         { "type": "keyword" },
          "is_anomaly":       { "type": "boolean" }
        }
      }
    }
  }' && echo ""

# ── Bootstrap write aliases (required for ILM rollover) ──────
echo "==> Creating initial write indices..."

# events-raw-000001
curl -s -X PUT "${OS_URL}/events-raw-000001" \
  -H "Content-Type: application/json" \
  -d '{
    "aliases": {
      "events-raw": { "is_write_index": true }
    }
  }' && echo ""

# events-anomalies-000001
curl -s -X PUT "${OS_URL}/events-anomalies-000001" \
  -H "Content-Type: application/json" \
  -d '{
    "aliases": {
      "events-anomalies": { "is_write_index": true }
    }
  }' && echo ""

echo "==> OpenSearch initialization complete."
