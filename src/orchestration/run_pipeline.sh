#!/bin/bash

# ==============================================================================
# ADVANCED SHELL DEBUGGING FLAGS
# ==============================================================================
set -e          # Exit immediately if any command exits with a non-zero status.
set -o pipefail # Captures failures hidden inside text pipes (|).
# set -x        # UNCOMMENT THIS LINE ONLY FOR DEEP TRACING (Prints every execution step live)

# ==============================================================================
# Enterprise Lakehouse Orchestration Control Loop
# Core Focus: Lifecycle Automation & Subprocess Tracking
# ==============================================================================

# Define process tracking arrays
PRODUCER_PID=""
CONSUMER_PID=""

cleanup_pipeline() {
    echo -e "\n\n[ORCHESTRATOR] 🛑 Shutdown signal received! Initiating safe cleanup sequence..."
    
    # Debugging Checkpoint: Terminate the background streaming tasks cleanly
    if [ ! -z "$PRODUCER_PID" ]; then
        echo "[ORCHESTRATOR] Terminating Emitter Simulator (PID: $PRODUCER_PID)..."
        kill -15 $PRODUCER_PID 2>/dev/null
    fi
    
    if [ ! -z "$CONSUMER_PID" ]; then
        echo "[ORCHESTRATOR] Terminating Data Lake Consumer (PID: $CONSUMER_PID)..."
        kill -15 $CONSUMER_PID 2>/dev/null
    fi
    
    echo "[ORCHESTRATOR] ✅ All background streaming processes dismantled safely. Core pipeline idle."
    exit 0
}

# Trap system interruption events (like Ctrl+C) and route them to our cleanup function
trap cleanup_pipeline SIGINT SIGTERM

echo "======================================================================"
echo "🚀 STARTING GEOSPATIAL IOT LAKEHOUSE AUTOMATED PIPELINE"
echo "======================================================================"

# Step 1: Verify Infrastructure Health
echo "[ORCHESTRATOR] Checking Kafka Broker status..."
if ! docker ps | grep -q "kafka-broker"; then
    echo "❌ CRITICAL ERROR: Kafka container is down. Run 'docker compose up -d' first."
    exit 1
fi
echo "[ORCHESTRATOR] ✅ Kafka Infrastructure Online."

# Step 2: Launch Stream Extraction Layer in the Background
echo "[ORCHESTRATOR] Activating Stream Emitter..."
python src/generators/iot_emitter.py &
PRODUCER_PID=$! # Capture the exact Linux system process ID of the script
echo "[ORCHESTRATOR] Emitter active under Process ID: $PRODUCER_PID"

echo "[ORCHESTRATOR] Activating Data Lake Bronze Consumer..."
python src/consumers/lake_consumer.py &
CONSUMER_PID=$! # Capture the exact Linux system process ID of the script
echo "[ORCHESTRATOR] Consumer active under Process ID: $CONSUMER_PID"

# Step 3: Let the stream run to collect data batches
RUN_DURATION_SECS=30
echo "======================================================================"
echo "[ORCHESTRATOR] ⏳ Ingestion active. Collecting telemetry stream for $RUN_DURATION_SECS seconds..."
echo "======================================================================"

sleep $RUN_DURATION_SECS

# Step 4: Gracefully halt the stream collectors to finalize the text files
echo "[ORCHESTRATOR] Halting streaming ingestion windows to lock down Bronze partition blocks..."
kill -15 $PRODUCER_PID 2>/dev/null
kill -15 $CONSUMER_PID 2>/dev/null
sleep 2

# Step 5: Trigger the PySpark Batch Transformation Engine
echo "======================================================================"
echo "[ORCHESTRATOR] ⚡ Launching PySpark Compaction Engine (Bronze -> Silver)..."
echo "======================================================================"
python src/transformers/bronze_to_silver.py

# Step 6: Trigger the Gold Analytical AI Dispatcher
echo "======================================================================"
echo "[ORCHESTRATOR] 🤖 Launching Gemini 3.5 Flash Gold Layer Dispatcher..."
echo "======================================================================"
python src/analytics/gold_ai_dispatcher.py

echo "======================================================================"
echo "🎉 PIPELINE BATCH EXECUTION COMPLETELY SUCCESSFUL"
echo "======================================================================"