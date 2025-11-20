#!/bin/bash
#
# Arquivo.pt CDXJ Collection Processing Pipeline
# Reference implementation using Unix pipes
#
# This script processes a complete web archive collection:
# 1. Index WARCs in parallel → 2. Merge → 3. Filter blocklist → 4. Filter excessive URLs → 5. Convert to ZipNum
#
# Usage:
#   ./process-collection.sh <collection_name> [options]
#
# Examples:
#   ./process-collection.sh COLLECTION-2024-11
#   ./process-collection.sh COLLECTION-2024-11 --blocklist /path/to/blocklist.txt
#   ./process-collection.sh COLLECTION-2024-11 --threshold 5000 --jobs 16
#
# Requirements:
#   - GNU parallel (sudo apt-get install parallel)
#   - pywb with cdx-indexer (pip install pywb)
#   - replay-cdxj-indexing-tools (pip install -e .)

set -e          # Exit on error
set -o pipefail # Exit if any pipe command fails
set -u          # Exit on undefined variable

# Configuration defaults
COLLECTIONS_BASE="/data/collections"
BLOCKLIST_DIR="/data/blocklists"
OUTPUT_BASE="/data/zipnum"
TEMP_BASE="/data/cdxj-processing-tmp"
DEFAULT_BLOCKLIST="arquivo-blocklist.txt"
EXCESSIVE_THRESHOLD=1000
PARALLEL_JOBS=$(nproc)
SHARD_SIZE=3000

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Usage information
usage() {
    cat << EOF
Usage: $0 <collection_name> [options]

Process an Arquivo.pt web archive collection through the complete CDXJ pipeline.

Arguments:
  collection_name       Name of the collection (e.g., COLLECTION-2024-11)

Options:
  --blocklist FILE      Path to blocklist file (default: $BLOCKLIST_DIR/$DEFAULT_BLOCKLIST)
  --threshold N         Excessive URLs threshold (default: $EXCESSIVE_THRESHOLD)
  --jobs N              Number of parallel jobs (default: $PARALLEL_JOBS)
  --shard-size N        ZipNum shard size (default: $SHARD_SIZE)
  --collections-dir DIR Base directory for collections (default: $COLLECTIONS_BASE)
  --output-dir DIR      Output directory base (default: $OUTPUT_BASE)
  --temp-dir DIR        Temporary directory base (default: $TEMP_BASE)
  --keep-temp           Keep temporary files after processing
  --no-compress         Don't compress ZipNum output
  --incremental         Only index new/modified WARCs (for daily updates)
  --help                Show this help message

Examples:
  # Process collection with defaults
  $0 COLLECTION-2024-11

  # Process with custom blocklist and threshold
  $0 COLLECTION-2024-11 --blocklist /path/to/custom.txt --threshold 5000

  # Process with more parallel jobs
  $0 COLLECTION-2024-11 --jobs 32

  # Process and keep intermediate files
  $0 COLLECTION-2024-11 --keep-temp

  # Daily incremental update (only index new WARCs)
  $0 COLLECTION-2024-11 --incremental

Directory Structure:
  Input:  $COLLECTIONS_BASE/<collection_name>/*.warc.gz
  Output: $OUTPUT_BASE/<collection_name>/
  Temp:   $TEMP_BASE/<collection_name>/

Pipeline Stages:
  1. Index WARCs in parallel using cdx-indexer
  2. Merge all CDXJ indexes
  3. Filter using blocklist (spam, adult content, legal removals)
  4. Filter excessive URLs (crawler traps, spam sites)
  5. Convert to ZipNum format for pywb

EOF
    exit 1
}

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

# Check dependencies
check_dependencies() {
    local missing=0
    
    if ! command -v parallel &> /dev/null; then
        log_error "GNU parallel not found. Install: sudo apt-get install parallel"
        missing=1
    fi
    
    if ! command -v cdx-indexer &> /dev/null; then
        log_error "cdx-indexer not found. Install: pip install pywb"
        missing=1
    fi
    
    if ! command -v merge-flat-cdxj &> /dev/null; then
        log_error "merge-flat-cdxj not found. Install: pip install -e ."
        missing=1
    fi
    
    if ! command -v filter-blocklist &> /dev/null; then
        log_error "filter-blocklist not found. Install: pip install -e ."
        missing=1
    fi
    
    if ! command -v filter-excessive-urls &> /dev/null; then
        log_error "filter-excessive-urls not found. Install: pip install -e ."
        missing=1
    fi
    
    if ! command -v flat-cdxj-to-zipnum &> /dev/null; then
        log_error "flat-cdxj-to-zipnum not found. Install: pip install -e ."
        missing=1
    fi
    
    if [ $missing -eq 1 ]; then
        log_error "Missing required dependencies. Please install them and try again."
        exit 1
    fi
}

# Parse command line arguments
COLLECTION_NAME=""
BLOCKLIST=""
KEEP_TEMP=0
COMPRESS="--compress"
INCREMENTAL=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --help)
            usage
            ;;
        --blocklist)
            BLOCKLIST="$2"
            shift 2
            ;;
        --threshold)
            EXCESSIVE_THRESHOLD="$2"
            shift 2
            ;;
        --jobs)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        --shard-size)
            SHARD_SIZE="$2"
            shift 2
            ;;
        --collections-dir)
            COLLECTIONS_BASE="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_BASE="$2"
            shift 2
            ;;
        --temp-dir)
            TEMP_BASE="$2"
            shift 2
            ;;
        --keep-temp)
            KEEP_TEMP=1
            shift
            ;;
        --no-compress)
            COMPRESS=""
            shift
            ;;
        --incremental)
            INCREMENTAL=1
            shift
            ;;
        -*)
            log_error "Unknown option: $1"
            usage
            ;;
        *)
            if [ -z "$COLLECTION_NAME" ]; then
                COLLECTION_NAME="$1"
            else
                log_error "Too many arguments"
                usage
            fi
            shift
            ;;
    esac
done

# Validate collection name
if [ -z "$COLLECTION_NAME" ]; then
    log_error "Collection name is required"
    usage
fi

# Set paths
COLLECTION_DIR="$COLLECTIONS_BASE/$COLLECTION_NAME"
TEMP_DIR="$TEMP_BASE/$COLLECTION_NAME"
INDEXES_DIR="$TEMP_DIR/indexes"
OUTPUT_DIR="$OUTPUT_BASE/$COLLECTION_NAME"

# Set blocklist path
if [ -z "$BLOCKLIST" ]; then
    BLOCKLIST="$BLOCKLIST_DIR/$DEFAULT_BLOCKLIST"
fi

# Check if collection exists
if [ ! -d "$COLLECTION_DIR" ]; then
    log_error "Collection directory not found: $COLLECTION_DIR"
    exit 1
fi

# Count WARC files
WARC_COUNT=$(find "$COLLECTION_DIR" -name "*.warc.gz" -o -name "*.warc" | wc -l)
if [ "$WARC_COUNT" -eq 0 ]; then
    log_error "No WARC files found in $COLLECTION_DIR"
    exit 1
fi

# Check blocklist
if [ ! -f "$BLOCKLIST" ]; then
    log_warning "Blocklist not found: $BLOCKLIST"
    log_warning "Proceeding without blocklist filtering"
    USE_BLOCKLIST=0
else
    USE_BLOCKLIST=1
fi

# Check dependencies
check_dependencies

# Display configuration
echo ""
echo "=========================================="
echo "Arquivo.pt Collection Processing Pipeline"
echo "=========================================="
echo ""
log_info "Configuration:"
echo "  Collection:         $COLLECTION_NAME"
echo "  WARC files:         $WARC_COUNT"
echo "  Collection dir:     $COLLECTION_DIR"
echo "  Output dir:         $OUTPUT_DIR"
echo "  Temp dir:           $TEMP_DIR"
echo "  Blocklist:          $BLOCKLIST"
echo "  Excessive threshold: $EXCESSIVE_THRESHOLD"
echo "  Parallel jobs:      $PARALLEL_JOBS"
echo "  Shard size:         $SHARD_SIZE"
echo "  Incremental mode:   $INCREMENTAL"
echo "  Keep temp files:    $KEEP_TEMP"
echo ""

# Start timing
START_TIME=$(date +%s)
log_info "Started at $(date)"
echo ""

# Create directories
log_info "Creating directories..."
mkdir -p "$INDEXES_DIR"
mkdir -p "$OUTPUT_DIR"

# Clean up any leftover temporary files from previous interrupted runs
TMP_COUNT=$(find "$INDEXES_DIR" -name "*.cdxj.tmp" 2>/dev/null | wc -l)
if [ $TMP_COUNT -gt 0 ]; then
    log_warning "Found $TMP_COUNT incomplete index files from previous run"
    log_info "Cleaning up temporary files..."
    find "$INDEXES_DIR" -name "*.cdxj.tmp" -delete
    log_success "Cleaned up $TMP_COUNT temporary files"
fi

# Error handling
cleanup_on_error() {
    log_error "Pipeline failed at line $1"
    if [ $KEEP_TEMP -eq 0 ]; then
        log_info "Cleaning up temporary files..."
        rm -rf "$TEMP_DIR"
    else
        log_info "Temporary files kept at: $TEMP_DIR"
    fi
    exit 1
}

trap 'cleanup_on_error $LINENO' ERR

# ============================================
# STAGE 1: Index WARCs in Parallel
# ============================================
echo ""
if [ $INCREMENTAL -eq 1 ]; then
    log_info "STAGE 1/5: Incremental indexing - only processing new/modified WARCs ($PARALLEL_JOBS jobs)..."
else
    log_info "STAGE 1/5: Indexing ALL WARC files in parallel ($PARALLEL_JOBS jobs)..."
fi
echo ""

# Function to index a single WARC (used by parallel)
# 
# Safety mechanism: Uses atomic writes to prevent corrupted index files
# - Indexes to .cdxj.tmp first
# - Only moves to .cdxj if indexing completes successfully
# - If interrupted, .tmp files are cleaned up on next run
# - Prevents incremental mode from skipping corrupted/incomplete indexes
index_warc() {
    local warc_path="$1"
    local indexes_dir="$2"
    local incremental="$3"
    
    # Get relative path and create output path
    local warc_basename=$(basename "$warc_path")
    local cdxj_path="$indexes_dir/${warc_basename}.cdxj"
    local cdxj_tmp="$cdxj_path.tmp"
    
    # Incremental mode: skip if CDXJ exists and is newer than WARC
    if [ "$incremental" -eq 1 ] && [ -f "$cdxj_path" ] && [ "$cdxj_path" -nt "$warc_path" ]; then
        echo "  [SKIP] $warc_basename (already indexed)" >&2
        return 0
    fi
    
    # Clean up any existing temporary file (from previous interrupted run)
    rm -f "$cdxj_tmp"
    
    # Index the WARC to temporary file (atomic operation)
    echo "  [INDEX] $warc_basename" >&2
    touch "$cdxj_tmp"
    cdx-indexer --postappend --cdxj "$warc_path" -o "$cdxj_tmp" 2>/dev/null
    
    local retval=$?
    if [ $retval -ne 0 ]; then
        echo "  [ERROR] Failed to index $warc_basename" >&2
        rm -f "$cdxj_tmp"
        return 1
    fi
    
    # Atomic move: only replace final file if indexing completed successfully
    # This prevents partial/corrupted CDXJ files from being left behind
    mv "$cdxj_tmp" "$cdxj_path"
    
    return 0
}

# Export function and variables for parallel
export -f index_warc
export INDEXES_DIR
export INCREMENTAL

# Run indexing in parallel
find "$COLLECTION_DIR" -name "*.warc.gz" -o -name "*.warc" | \
    parallel -j "$PARALLEL_JOBS" --bar --eta \
    index_warc {} "$INDEXES_DIR" "$INCREMENTAL"

# Count results
CDXJ_COUNT=$(ls "$INDEXES_DIR"/*.cdxj 2>/dev/null | wc -l)
if [ $INCREMENTAL -eq 1 ]; then
    NEW_COUNT=$(find "$COLLECTION_DIR" \( -name "*.warc.gz" -o -name "*.warc" \) -newer "$INDEXES_DIR" 2>/dev/null | wc -l)
    log_success "Processed: $CDXJ_COUNT total indexes ($NEW_COUNT new/modified)"
else
    log_success "Created $CDXJ_COUNT CDXJ index files"
fi

# ============================================
# STAGE 2: Filter Blocklist in Parallel (if enabled)
# ============================================
if [ $USE_BLOCKLIST -eq 1 ]; then
    echo ""
    log_info "STAGE 2/5: Filtering blocklist in parallel ($PARALLEL_JOBS jobs)..."
    log_info "OPTIMIZATION: Filter before merge for 4x speedup"
    echo ""
    
    FILTERED_DIR="$TEMP_DIR/filtered"
    mkdir -p "$FILTERED_DIR"
    
    # Function to filter a single CDXJ file
    filter_cdxj() {
        local input_file="$1"
        local blocklist="$2"
        local output_dir="$3"
        
        local basename=$(basename "$input_file")
        local output_file="$output_dir/$basename"
        
        # Filter this file
        filter-blocklist -i "$input_file" -b "$blocklist" -o "$output_file" 2>/dev/null
        
        if [ $? -eq 0 ]; then
            echo "  [FILTERED] $basename" >&2
        else
            echo "  [ERROR] Failed to filter $basename" >&2
            return 1
        fi
    }
    
    # Export for parallel
    export -f filter_cdxj
    export BLOCKLIST
    export FILTERED_DIR
    
    # Filter all CDXJ files in parallel
    find "$INDEXES_DIR" -name "*.cdxj" | \
        parallel -j "$PARALLEL_JOBS" --bar --eta \
        filter_cdxj {} "$BLOCKLIST" "$FILTERED_DIR"
    
    FILTERED_COUNT=$(ls "$FILTERED_DIR"/*.cdxj 2>/dev/null | wc -l)
    log_success "Filtered $FILTERED_COUNT files in parallel"
    
    # Use filtered files for subsequent stages
    PROCESS_DIR="$FILTERED_DIR"
else
    # No blocklist - use original indexes
    PROCESS_DIR="$INDEXES_DIR"
fi

# ============================================
# STAGE 3-5: Pipeline Processing
# ============================================
echo ""
if [ $USE_BLOCKLIST -eq 1 ]; then
    log_info "STAGE 3-5: Processing pipeline (merge filtered → filter excessive → zipnum)..."
else
    log_info "STAGE 2-5: Processing pipeline (merge → filter excessive → zipnum)..."
fi
log_info "This uses Unix pipes for efficient streaming processing"
echo ""

# Build pipeline command (using pre-filtered files if blocklist was applied)
log_info "Pipeline: merge → excessive → zipnum"

merge-flat-cdxj - "$PROCESS_DIR"/*.cdxj | \
    tee >(wc -l | xargs -I {} echo -e "  ${BLUE}→${NC} After merge: {} lines" >&2) | \
    filter-excessive-urls auto -i - -n "$EXCESSIVE_THRESHOLD" -v 2>&1 | \
    tee >(grep "output" | sed "s/^/  ${BLUE}→${NC} /" >&2) | \
    flat-cdxj-to-zipnum -o "$OUTPUT_DIR" -i - -n "$SHARD_SIZE" $COMPRESS

echo ""
log_success "Pipeline processing complete"

# ============================================
# Cleanup
# ============================================
echo ""
if [ $KEEP_TEMP -eq 0 ]; then
    log_info "Cleaning up temporary files..."
    rm -rf "$TEMP_DIR"
    log_success "Temporary files removed"
else
    log_info "Temporary files kept at: $TEMP_DIR"
fi

# ============================================
# Final Statistics
# ============================================
echo ""
echo "=========================================="
echo "Processing Complete"
echo "=========================================="
echo ""

# Count output files
SHARD_COUNT=$(find "$OUTPUT_DIR/index.cdxj" -name "*.gz" -o -name "*.cdxj" 2>/dev/null | wc -l)
OUTPUT_SIZE=$(du -sh "$OUTPUT_DIR" | cut -f1)

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
HOURS=$((DURATION / 3600))
MINUTES=$(((DURATION % 3600) / 60))
SECONDS=$((DURATION % 60))

log_success "Statistics:"
echo "  Input WARC files:   $WARC_COUNT"
echo "  Output shards:      $SHARD_COUNT"
echo "  Output size:        $OUTPUT_SIZE"
echo "  Processing time:    ${HOURS}h ${MINUTES}m ${SECONDS}s"
echo ""
log_success "Output directory:   $OUTPUT_DIR"
echo ""
log_info "Finished at $(date)"
echo ""

# ============================================
# Usage Instructions
# ============================================
echo "Next Steps:"
echo ""
echo "1. Verify the output:"
echo "   ls -lh $OUTPUT_DIR/index.cdxj/"
echo ""
echo "2. Test with pywb:"
echo "   wb-manager add collection $OUTPUT_DIR"
echo ""
echo "3. View sample index entries:"
echo "   zcat $OUTPUT_DIR/index.cdxj/*.gz | head -10"
echo ""

exit 0
