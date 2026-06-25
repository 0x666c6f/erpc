#!/bin/bash
set -eo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse command line arguments (PARALLEL_JOBS set after chart discovery)
PARALLEL_JOBS=""
while getopts "j:sh" opt; do
  case $opt in
    j) PARALLEL_JOBS="$OPTARG" ;;
    s) SEQUENTIAL=true ;;
    h)
      echo "Usage: $0 [-j N] [-s] [-h]"
      echo "  -j N  Run N parallel jobs (default: number of charts)"
      echo "  -s    Run sequentially (disable parallelism)"
      echo "  -h    Show this help"
      exit 0
      ;;
    \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
  esac
done

# Check if kubeconform is installed
if ! command -v kubeconform &> /dev/null; then
    echo -e "${RED}kubeconform is not installed. Please install it first.${NC}"
    echo "You can install it via: https://github.com/yannh/kubeconform#installation"
    exit 1
fi

# Check if helm is installed
if ! command -v helm &> /dev/null; then
    echo -e "${RED}helm is not installed. Please install it first.${NC}"
    echo "You can install it via: https://helm.sh/docs/intro/install/"
    exit 1
fi

# Store the initial working directory
INITIAL_DIR=$(pwd)
export INITIAL_DIR

# Set base directories to scan (erpc: base chart + prd env wrapper only)
DIRS_TO_SCAN=("helm/charts/erpc" "helm/environments/prd/erpc")

# List of custom resources to skip
CUSTOM_RESOURCES="PodMonitor,ServiceMonitor,AlertManager,Prometheus,PrometheusRule,Cluster,ScheduledBackup,ImageCatalog,IngressRoute,Certificate,CronJob,EventSource,EventBus,Sensor"
export CUSTOM_RESOURCES

# List of charts to explicitly skip validation
SKIP_VALIDATION_CHARTS=()

# Function to check if a chart should skip validation
should_skip_validation() {
  local chart_name=$1

  # Check explicit list
  for skip_chart in "${SKIP_VALIDATION_CHARTS[@]}"; do
    if [[ "$chart_name" == "$skip_chart" ]]; then
      return 0 # true, should skip
    fi
  done

  # Check database pattern
  if [[ "$chart_name" == *"-db" || "$chart_name" == *"database"* || "$chart_name" == *"postgres"* ]]; then
    return 0 # true, should skip
  fi

  return 1 # false, should not skip
}

# Export for parallel subshells
export -f should_skip_validation
export SKIP_VALIDATION_CHARTS

# Function to process a single chart (for parallel execution)
process_chart() {
    local chart_path=$1
    local chart_name=$(basename "$chart_path")
    local result_file=$(mktemp)
    local has_error=0

    # Store the absolute path to the chart
    local chart_absolute_path=$(realpath "$chart_path")

    echo "Processing: ${chart_path}" >&2

    # Run helm lint
    if ! helm lint "$chart_absolute_path" > "$result_file.lint" 2>&1; then
        echo -e "${RED}✗ Helm lint failed for ${chart_name}${NC}" >&2
        cat "$result_file.lint" >&2
        has_error=1
    else
        echo -e "${GREEN}✓ Helm lint passed for ${chart_name}${NC}" >&2
    fi

    # Check if this chart should skip validation
    if should_skip_validation "$chart_name"; then
        echo -e "${YELLOW}⊘ Skipping kubeconform for ${chart_name} (database chart)${NC}" >&2
        rm -f "$result_file" "$result_file.lint" "$result_file.template"
        return $has_error
    fi

    # Build dependencies if Chart.yaml exists and has dependencies
    if [ -f "${chart_absolute_path}/Chart.yaml" ]; then
        if grep -q "dependencies:" "${chart_absolute_path}/Chart.yaml"; then
            cd "$chart_absolute_path"
            if ! helm dependency update > /dev/null 2>&1; then
                echo -e "${RED}✗ Dependency update failed for ${chart_name}${NC}" >&2
                cd "$INITIAL_DIR"
                rm -f "$result_file" "$result_file.lint" "$result_file.template"
                return 1
            fi
        fi

        # Run template validation
        cd "$chart_absolute_path"
        if ! helm template . 2>/dev/null | kubeconform --ignore-missing-schemas --summary --skip "$CUSTOM_RESOURCES" > "$result_file.template" 2>&1; then
            echo -e "${RED}✗ Kubeconform failed for ${chart_name}${NC}" >&2
            cat "$result_file.template" >&2
            has_error=1
        else
            echo -e "${GREEN}✓ Kubeconform passed for ${chart_name}${NC}" >&2
        fi
        cd "$INITIAL_DIR"
    else
        echo -e "${RED}No Chart.yaml found in ${chart_path}${NC}" >&2
        has_error=1
    fi

    rm -f "$result_file" "$result_file.lint" "$result_file.template"
    return $has_error
}

# Export for parallel
export -f process_chart

# Main script execution
echo -e "${YELLOW}Starting Helm chart validation...${NC}"
echo -e "${YELLOW}Skipping validation for these custom resources: ${CUSTOM_RESOURCES}${NC}"

# Add common repositories that might be needed
echo -e "${YELLOW}Adding common Helm repositories...${NC}"
helm repo add bitnami https://charts.bitnami.com/bitnami 2>/dev/null || true
helm repo add cnpg https://cloudnative-pg.github.io/charts 2>/dev/null || true
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo add haproxytech https://haproxytech.github.io/helm-charts 2>/dev/null || true
helm repo add dandydeveloper https://dandydeveloper.github.io/charts 2>/dev/null || true

# Update Helm repositories
echo -e "${YELLOW}Updating Helm repositories...${NC}"
helm repo update

# Collect all charts to validate
CHARTS=()
for dir in "${DIRS_TO_SCAN[@]}"; do
    if [ ! -d "$dir" ]; then
        echo -e "${YELLOW}Directory $dir does not exist, skipping...${NC}"
        continue
    fi

    while IFS= read -r chart_yaml; do
        chart_dir=$(dirname "$chart_yaml")
        CHARTS+=("$chart_dir")
    done < <(find "$dir" -name Chart.yaml -type f)
done

TOTAL_CHARTS=${#CHARTS[@]}

# Set PARALLEL_JOBS to number of charts if not specified
if [ -z "$PARALLEL_JOBS" ]; then
    PARALLEL_JOBS=$TOTAL_CHARTS
fi

echo -e "\n${YELLOW}Found ${TOTAL_CHARTS} charts to validate${NC}\n"

# Run validation
FAILED_CHARTS=()

if [ "$SEQUENTIAL" = true ] || ! command -v parallel &> /dev/null; then
    # Sequential execution (fallback or explicit)
    if [ "$SEQUENTIAL" != true ]; then
        echo -e "${YELLOW}GNU parallel not found, falling back to sequential execution${NC}"
        echo -e "${YELLOW}Install with: brew install parallel (macOS) or apt install parallel (Linux)${NC}\n"
    fi

    for chart in "${CHARTS[@]}"; do
        if ! process_chart "$chart"; then
            FAILED_CHARTS+=("$chart")
        fi
    done
else
    # Parallel execution
    echo -e "${BLUE}Running ${PARALLEL_JOBS} parallel validation jobs...${NC}\n"

    # Create a temporary file to track failures
    FAIL_FILE=$(mktemp)

    # Run in parallel, collecting failures
    printf '%s\n' "${CHARTS[@]}" | \
        parallel -j "$PARALLEL_JOBS" --halt never,fail=1 --line-buffer \
            "process_chart {} || echo {} >> $FAIL_FILE"

    # Collect failed charts
    if [ -f "$FAIL_FILE" ]; then
        while IFS= read -r failed; do
            FAILED_CHARTS+=("$failed")
        done < "$FAIL_FILE"
        rm -f "$FAIL_FILE"
    fi
fi

# Summary report
echo -e "\n${YELLOW}==== Validation Summary ====${NC}"
echo -e "Total charts processed: ${TOTAL_CHARTS}"
echo -e "Failed charts: ${#FAILED_CHARTS[@]}"

if [ ${#FAILED_CHARTS[@]} -eq 0 ]; then
    echo -e "${GREEN}All validations passed successfully!${NC}"
    exit 0
else
    echo -e "${RED}The following charts failed validation:${NC}"
    for chart in "${FAILED_CHARTS[@]}"; do
        echo -e "  - ${chart}"
    done
    exit 1
fi
