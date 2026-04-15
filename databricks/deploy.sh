#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Idempotent Databricks deployment for the Predictive Maintenance
#             Digital Twin pipeline.
#
# Steps:
#   1. Validate environment and prerequisites
#   2. Run Unity Catalog setup (Python)
#   3. Deploy / update the Databricks workflow job
#   4. Optionally trigger a manual validation run
#   5. Print a deployment summary
#
# Usage:
#   ./databricks/deploy.sh [--dry-run] [--env dev|prod] [--no-run]
#
# Environment variables (required unless --dry-run):
#   DATABRICKS_HOST     Workspace URL (e.g. https://dbc-xxx.cloud.databricks.com)
#   DATABRICKS_TOKEN    Personal access token or service principal OAuth token
#   ALERT_EMAIL         Email for job failure notifications
# =============================================================================

set -euo pipefail

# ── Colour helpers ─────────────────────────────────────────────────────────────
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
CYAN="\033[0;36m"
RESET="\033[0m"

info()    { echo -e "${GREEN}[INFO]${RESET}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; }
header()  { echo -e "\n${CYAN}══════════════════════════════════════════${RESET}"; echo -e "${CYAN}  $*${RESET}"; echo -e "${CYAN}══════════════════════════════════════════${RESET}"; }

# ── Defaults ───────────────────────────────────────────────────────────────────
DRY_RUN=false
ENV="prod"
TRIGGER_RUN=true
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# ── Argument parsing ───────────────────────────────────────────────────────────
usage() {
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --dry-run         Log all actions without executing them"
    echo "  --env dev|prod    Target environment (default: prod)"
    echo "  --no-run          Skip the manual validation run after deploy"
    echo "  --workspace-url   Override DATABRICKS_HOST env var"
    echo "  -h, --help        Show this help message"
    echo ""
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)      DRY_RUN=true;          shift ;;
        --env)          ENV="$2";               shift 2 ;;
        --no-run)       TRIGGER_RUN=false;      shift ;;
        --workspace-url) DATABRICKS_HOST="$2";  shift 2 ;;
        -h|--help)      usage ;;
        *)              error "Unknown argument: $1"; usage ;;
    esac
done

# ── Pre-flight checks ──────────────────────────────────────────────────────────
header "Pre-flight Checks"

check_env_var() {
    local var_name="$1"
    local value="${!var_name:-}"
    if [[ -z "$value" ]] && [[ "$DRY_RUN" == "false" ]]; then
        error "Required environment variable '$var_name' is not set."
        error "Set it in your shell or .env file before running this script."
        exit 1
    elif [[ -z "$value" ]]; then
        warn "$var_name is not set (OK in dry-run mode)"
    else
        info "$var_name is set"
    fi
}

check_env_var "DATABRICKS_HOST"
check_env_var "DATABRICKS_TOKEN"
check_env_var "ALERT_EMAIL"

# Check Databricks CLI
if ! command -v databricks &> /dev/null; then
    error "Databricks CLI not found. Install with: pip install databricks-cli"
    error "Then configure with: databricks configure --token"
    exit 1
fi
info "Databricks CLI found: $(databricks --version 2>/dev/null || echo 'version unknown')"

# Check Python
PYTHON_CMD="python3"
if ! command -v python3 &> /dev/null; then
    if ! command -v python &> /dev/null; then
        error "python / python3 not found"
        exit 1
    fi
    PYTHON_CMD="python"
fi
info "Python: $($PYTHON_CMD --version)"

info "Environment: ${ENV}"
info "Dry-run: ${DRY_RUN}"

# ── Step 1: Unity Catalog Setup ────────────────────────────────────────────────
header "Step 1: Unity Catalog Setup"

DRY_FLAG=""
[[ "$DRY_RUN" == "true" ]] && DRY_FLAG="--dry-run"

if [[ "$DRY_RUN" == "true" ]]; then
    warn "[DRY-RUN] Would run: $PYTHON_CMD databricks/unity_catalog_setup.py --dry-run"
else
    info "Running Unity Catalog setup..."
    $PYTHON_CMD "${PROJECT_ROOT}/databricks/unity_catalog_setup.py" \
        --workspace-url "${DATABRICKS_HOST}" \
        ${DRY_FLAG} \
        && info "Unity Catalog setup complete" \
        || { error "Unity Catalog setup failed"; exit 1; }
fi

# ── Step 2: Deploy / Update Workflow Job ──────────────────────────────────────
header "Step 2: Deploy Databricks Workflow Job"

WORKFLOW_CONFIG="${SCRIPT_DIR}/workflow_config.json"

if [[ ! -f "$WORKFLOW_CONFIG" ]]; then
    error "workflow_config.json not found at: $WORKFLOW_CONFIG"
    exit 1
fi

# Substitute ALERT_EMAIL into the workflow config
TEMP_CONFIG=$(mktemp /tmp/pmt_workflow_XXXXXX.json)
sed "s/\${ALERT_EMAIL}/${ALERT_EMAIL}/g" "$WORKFLOW_CONFIG" > "$TEMP_CONFIG"
trap 'rm -f "$TEMP_CONFIG"' EXIT

if [[ "$DRY_RUN" == "true" ]]; then
    warn "[DRY-RUN] Would deploy workflow from: $WORKFLOW_CONFIG"
    warn "[DRY-RUN] ALERT_EMAIL would be substituted with: ${ALERT_EMAIL:-<not set>}"
else
    info "Deploying workflow job..."
    JOB_ID=$(databricks jobs create-or-update \
        --job-file "$TEMP_CONFIG" \
        2>/dev/null | jq -r '.job_id // empty' || true)

    if [[ -n "$JOB_ID" ]]; then
        info "Workflow deployed: job_id=$JOB_ID"
    else
        warn "Could not parse job_id from CLI output — job may still have deployed"
        JOB_ID="unknown"
    fi
fi

# ── Step 3: Trigger Validation Run ────────────────────────────────────────────
header "Step 3: Trigger Validation Run"

if [[ "$TRIGGER_RUN" == "false" ]]; then
    warn "Skipping validation run (--no-run specified)"
elif [[ "$DRY_RUN" == "true" ]]; then
    warn "[DRY-RUN] Would trigger one manual run of job ${JOB_ID:-<id unknown in dry-run>}"
else
    info "Triggering one manual run for validation..."
    RUN_ID=$(databricks jobs run-now \
        --job-id "${JOB_ID}" \
        2>/dev/null | jq -r '.run_id // empty' || true)

    if [[ -n "$RUN_ID" ]]; then
        info "Run triggered: run_id=$RUN_ID"
        RUN_URL="${DATABRICKS_HOST}/#job/${JOB_ID}/run/${RUN_ID}"
        info "Monitor at: ${RUN_URL}"
    else
        warn "Could not parse run_id — check the Databricks UI manually"
    fi
fi

# ── Step 4: Deployment Summary ─────────────────────────────────────────────────
header "Deployment Summary"

echo ""
echo -e "  ${GREEN}Project${RESET}    Predictive Maintenance Digital Twin"
echo -e "  ${GREEN}Env${RESET}        ${ENV}"
echo -e "  ${GREEN}Workspace${RESET}  ${DATABRICKS_HOST:-<not set>}"
echo -e "  ${GREEN}Job ID${RESET}     ${JOB_ID:-<dry-run>}"
echo -e "  ${GREEN}Run ID${RESET}     ${RUN_ID:-<not triggered>}"
if [[ -n "${JOB_ID:-}" ]] && [[ "$JOB_ID" != "unknown" ]]; then
    echo -e "  ${GREEN}Job URL${RESET}    ${DATABRICKS_HOST}/#job/${JOB_ID}"
fi
if [[ -n "${RUN_URL:-}" ]]; then
    echo -e "  ${GREEN}Run URL${RESET}    ${RUN_URL}"
fi
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
    warn "DRY-RUN mode — no changes were made to the Databricks workspace"
else
    info "Deployment complete!"
fi
