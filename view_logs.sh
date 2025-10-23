#!/bin/bash

# Log Viewer Script
# This script helps view logs from various components in the halo-cmm-dev environment

set -e

# Configuration
PROJECT="halo-cmm-dev"
REGION="us-central1"
INSTANCE_GROUP="results-fulfiller-mig-v2"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_header() {
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}========================================${NC}"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to view requisition-fetcher Cloud Function logs
view_requisition_fetcher_logs() {
    local limit=${1:-50}
    local freshness=${2:-2h}

    print_header "Requisition Fetcher Cloud Function Logs"
    print_info "Fetching last $limit log entries from past $freshness"
    echo ""

    gcloud logging read \
        'resource.type="cloud_run_revision" AND resource.labels.service_name="requisition-fetcher"' \
        --project="$PROJECT" \
        --limit="$limit" \
        --format="table(timestamp,severity,textPayload)" \
        --freshness="$freshness"
}

# Function to view requisition-fetcher application logs (structured)
view_requisition_fetcher_app_logs() {
    local limit=${1:-20}
    local freshness=${2:-24h}

    print_header "Requisition Fetcher Application Logs"
    print_info "Fetching application-level logs from past $freshness"
    echo ""

    gcloud logging read \
        'resource.type="cloud_run_revision" AND resource.labels.service_name="requisition-fetcher" AND jsonPayload.message!=""' \
        --project="$PROJECT" \
        --limit="$limit" \
        --format=json \
        --freshness="$freshness" | \
        jq -r '.[] | "\(.timestamp) [\(.severity)] \(.jsonPayload.message // .textPayload)"' | head -100
}

# Function to view results-fulfiller instance logs
view_results_fulfiller_logs() {
    local limit=${1:-50}
    local freshness=${2:-2h}

    print_header "Results Fulfiller Instance Logs (Serial Port 1)"
    print_info "Fetching last $limit log entries from past $freshness"
    echo ""

    gcloud logging read \
        'resource.type="gce_instance" AND logName:"serialconsole.googleapis.com%2Fserial_port_1_output"' \
        --project="$PROJECT" \
        --limit="$limit" \
        --format="table(timestamp,resource.labels.instance_id,textPayload.slice(0:150))" \
        --freshness="$freshness"
}

# Function to view OOM errors from results-fulfiller instances
view_oom_errors() {
    local limit=${1:-20}
    local freshness=${2:-24h}

    print_header "Out of Memory Errors"
    print_info "Searching for OOM errors in past $freshness"
    echo ""

    local count=$(gcloud logging read \
        'resource.type="gce_instance" AND logName:"serialconsole.googleapis.com%2Fserial_port_1_output" AND textPayload=~"oom-killer"' \
        --project="$PROJECT" \
        --limit="$limit" \
        --format="value(timestamp)" \
        --freshness="$freshness" | wc -l)

    if [ "$count" -eq 0 ]; then
        print_info "No OOM errors found"
    else
        print_error "Found $count OOM errors!"
        echo ""
        gcloud logging read \
            'resource.type="gce_instance" AND logName:"serialconsole.googleapis.com%2Fserial_port_1_output" AND textPayload=~"oom-killer"' \
            --project="$PROJECT" \
            --limit="$limit" \
            --format="table(timestamp,resource.labels.instance_id)" \
            --freshness="$freshness"
    fi
}

# Function to view Cloud Scheduler logs
view_scheduler_logs() {
    local limit=${1:-20}
    local freshness=${2:-24h}

    print_header "Cloud Scheduler Logs"
    print_info "Fetching scheduler execution logs from past $freshness"
    echo ""

    gcloud logging read \
        'resource.type="cloud_scheduler_job" AND resource.labels.job_id="requisition-fetcher-scheduler-requisition-fetcher"' \
        --project="$PROJECT" \
        --limit="$limit" \
        --format="table(timestamp,severity,jsonPayload.message,textPayload)" \
        --freshness="$freshness"
}

# Function to view Pub/Sub subscription metrics
view_pubsub_status() {
    print_header "Pub/Sub Queue Status"

    print_info "Checking results-fulfiller-subscription..."
    local main_count=$(gcloud pubsub subscriptions pull results-fulfiller-subscription \
        --project="$PROJECT" \
        --limit=1 \
        --format="value(MESSAGE_ID)" 2>/dev/null | wc -l)
    echo "  Messages in queue: $main_count"

    print_info "Checking results-fulfiller-queue-dlq-sub..."
    local dlq_count=$(gcloud pubsub subscriptions pull results-fulfiller-queue-dlq-sub \
        --project="$PROJECT" \
        --limit=1 \
        --format="value(MESSAGE_ID)" 2>/dev/null | wc -l)
    echo "  Messages in DLQ: $dlq_count"
    echo ""
}

# Function to view instance group status
view_instance_status() {
    print_header "Results Fulfiller Instance Status"

    gcloud compute instance-groups managed list-instances "$INSTANCE_GROUP" \
        --project="$PROJECT" \
        --region="$REGION" \
        --format="table(instance.basename(),status,currentAction,lastAttempt.errors)"
    echo ""
}

# Function to follow logs in real-time
follow_requisition_fetcher_logs() {
    print_header "Following Requisition Fetcher Logs (Real-time)"
    print_info "Press Ctrl+C to stop"
    echo ""

    gcloud logging tail \
        'resource.type="cloud_run_revision" AND resource.labels.service_name="requisition-fetcher"' \
        --project="$PROJECT" \
        --format="table(timestamp,severity,textPayload,jsonPayload.message)"
}

# Function to follow results-fulfiller logs in real-time
follow_results_fulfiller_logs() {
    print_header "Following Results Fulfiller Logs (Real-time)"
    print_info "Press Ctrl+C to stop"
    echo ""

    gcloud logging tail \
        'resource.type="gce_instance" AND logName:"serialconsole.googleapis.com%2Fserial_port_1_output"' \
        --project="$PROJECT" \
        --format="table(timestamp,resource.labels.instance_id,textPayload.slice(0:120))"
}

# Function to search logs by keyword
search_logs() {
    local keyword="$1"
    local limit=${2:-50}
    local freshness=${3:-24h}

    if [ -z "$keyword" ]; then
        print_error "Keyword is required"
        return 1
    fi

    print_header "Searching Logs for: $keyword"
    print_info "Searching in past $freshness"
    echo ""

    gcloud logging read \
        "textPayload=~\"$keyword\" OR jsonPayload.message=~\"$keyword\"" \
        --project="$PROJECT" \
        --limit="$limit" \
        --format="table(timestamp,resource.type,severity,textPayload.slice(0:100),jsonPayload.message.slice(0:100))" \
        --freshness="$freshness"
}

# Function to show dashboard (overview of all components)
show_dashboard() {
    print_header "System Dashboard"

    # Instance status
    echo -e "${BLUE}Instance Group Status:${NC}"
    view_instance_status

    # Pub/Sub status
    view_pubsub_status

    # Recent OOM errors
    print_info "Checking for recent OOM errors (past 1h)..."
    local oom_count=$(gcloud logging read \
        'resource.type="gce_instance" AND logName:"serialconsole.googleapis.com%2Fserial_port_1_output" AND textPayload=~"oom-killer"' \
        --project="$PROJECT" \
        --limit=1 \
        --format="value(timestamp)" \
        --freshness=1h 2>/dev/null | wc -l)

    if [ "$oom_count" -eq 0 ]; then
        echo -e "  ${GREEN}✓${NC} No recent OOM errors"
    else
        echo -e "  ${RED}✗${NC} Found OOM errors in past hour"
    fi

    # Scheduler status
    print_info "Checking Cloud Scheduler status..."
    gcloud scheduler jobs describe requisition-fetcher-scheduler-requisition-fetcher \
        --location="$REGION" \
        --project="$PROJECT" \
        --format="table(name,state,schedule)" 2>/dev/null || echo "  Unable to fetch scheduler status"

    echo ""
}

# Show usage
usage() {
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  fetcher [limit] [freshness]     View requisition-fetcher logs (default: 50, 2h)"
    echo "  fetcher-app [limit] [freshness] View requisition-fetcher application logs (default: 20, 24h)"
    echo "  fulfiller [limit] [freshness]   View results-fulfiller instance logs (default: 50, 2h)"
    echo "  oom [limit] [freshness]         View OOM errors (default: 20, 24h)"
    echo "  scheduler [limit] [freshness]   View Cloud Scheduler logs (default: 20, 24h)"
    echo "  pubsub                          View Pub/Sub queue status"
    echo "  instances                       View instance group status"
    echo "  follow-fetcher                  Follow requisition-fetcher logs in real-time"
    echo "  follow-fulfiller                Follow results-fulfiller logs in real-time"
    echo "  search <keyword> [limit] [freshness]  Search logs for keyword (default: 50, 24h)"
    echo "  dashboard                       Show system dashboard (overview)"
    echo "  help                            Show this help message"
    echo ""
    echo "Freshness format: Ns (seconds), Nm (minutes), Nh (hours), Nd (days)"
    echo ""
    echo "Examples:"
    echo "  $0 fetcher                      # Show last 50 requisition-fetcher logs from past 2h"
    echo "  $0 fetcher 100 6h               # Show last 100 logs from past 6h"
    echo "  $0 fulfiller 30 1h              # Show last 30 fulfiller logs from past 1h"
    echo "  $0 oom                          # Check for OOM errors"
    echo "  $0 search \"error\" 100 12h       # Search for 'error' in past 12h"
    echo "  $0 follow-fetcher               # Follow fetcher logs in real-time"
    echo "  $0 dashboard                    # Show system overview"
}

# Main execution
case "${1:-}" in
    fetcher)
        view_requisition_fetcher_logs "${2:-50}" "${3:-2h}"
        ;;
    fetcher-app)
        view_requisition_fetcher_app_logs "${2:-20}" "${3:-24h}"
        ;;
    fulfiller)
        view_results_fulfiller_logs "${2:-50}" "${3:-2h}"
        ;;
    oom)
        view_oom_errors "${2:-20}" "${3:-24h}"
        ;;
    scheduler)
        view_scheduler_logs "${2:-20}" "${3:-24h}"
        ;;
    pubsub)
        view_pubsub_status
        ;;
    instances)
        view_instance_status
        ;;
    follow-fetcher)
        follow_requisition_fetcher_logs
        ;;
    follow-fulfiller)
        follow_results_fulfiller_logs
        ;;
    search)
        if [ -z "${2:-}" ]; then
            print_error "Keyword is required for search"
            echo ""
            usage
            exit 1
        fi
        search_logs "$2" "${3:-50}" "${4:-24h}"
        ;;
    dashboard)
        show_dashboard
        ;;
    help|--help|-h)
        usage
        ;;
    "")
        print_error "No command specified"
        echo ""
        usage
        exit 1
        ;;
    *)
        print_error "Unknown command: $1"
        echo ""
        usage
        exit 1
        ;;
esac
