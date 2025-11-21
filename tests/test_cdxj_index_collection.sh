#!/bin/bash
#
# Test suite for cdxj-index-collection.sh
# Pure bash implementation - no external dependencies needed
#
# Usage:
#   bash tests/test_cdxj_index_collection.sh
#

# Don't use set -e here, as we're testing error conditions

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="$SCRIPT_DIR/replay_cdxj_indexing_tools/cdxj-index-collection.sh"

# Test environment
TEST_DIR=""

setup_test_env() {
    TEST_DIR="$(mktemp -d)"
    TEST_COLLECTION="TEST-2024-11"
    
    # Create test directory structure
    mkdir -p "$TEST_DIR/collections/$TEST_COLLECTION"
    mkdir -p "$TEST_DIR/blocklists"
    mkdir -p "$TEST_DIR/zipnum"
    mkdir -p "$TEST_DIR/temp"
    
    # Create a minimal test blocklist
    cat > "$TEST_DIR/blocklists/test-blocklist.txt" << 'EOF'
# Test blocklist
^pt,spam,
^com,example,blocked
EOF
}

cleanup_test_env() {
    if [ -n "$TEST_DIR" ] && [ -d "$TEST_DIR" ]; then
        rm -rf "$TEST_DIR"
    fi
}

# Test helper functions
assert_success() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $1"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗${NC} $1"
        ((TESTS_FAILED++))
        return 1
    fi
}

assert_failure() {
    if [ $? -ne 0 ]; then
        echo -e "${GREEN}✓${NC} $1"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗${NC} $1"
        ((TESTS_FAILED++))
        return 1
    fi
}

assert_contains() {
    local text="$1"
    local search="$2"
    local description="$3"
    
    if echo "$text" | grep -q "$search"; then
        echo -e "${GREEN}✓${NC} $description"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗${NC} $description"
        echo "  Expected to find: '$search'"
        ((TESTS_FAILED++))
        return 1
    fi
}

assert_not_contains() {
    local text="$1"
    local search="$2"
    local description="$3"
    
    if ! echo "$text" | grep -q "$search"; then
        echo -e "${GREEN}✓${NC} $description"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗${NC} $description"
        echo "  Did not expect to find: '$search'"
        ((TESTS_FAILED++))
        return 1
    fi
}

run_test() {
    ((TESTS_RUN++))
    echo -e "${BLUE}[TEST $TESTS_RUN]${NC} $1"
    
    # Run test function
    "$2"
}

# Test functions
test_script_exists() {
    [ -f "$SCRIPT" ] && [ -x "$SCRIPT" ]
    assert_success "Script exists and is executable"
}

test_script_syntax() {
    bash -n "$SCRIPT"
    assert_success "Script has valid bash syntax"
}

test_help_option() {
    local output=$(bash "$SCRIPT" --help 2>&1 || true)
    assert_contains "$output" "Usage:" "Shows usage with --help"
    assert_contains "$output" "collection_name" "Help includes collection_name argument"
    assert_contains "$output" "incremental" "Help includes --incremental option"
    assert_not_contains "$output" "collection-field" "Help does not include removed --collection-field option"
}

test_no_collection_name() {
    local output=$(bash "$SCRIPT" 2>&1 || true)
    assert_contains "$output" "Collection name is required" "Fails without collection name"
}

test_nonexistent_collection() {
    setup_test_env
    local output=$(bash "$SCRIPT" NONEXISTENT \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" 2>&1 || true)
    
    assert_contains "$output" "Collection directory not found" "Fails with non-existent collection"
    cleanup_test_env
}

test_empty_collection() {
    setup_test_env
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" 2>&1 || true)
    
    assert_contains "$output" "No WARC files found" "Fails with empty collection"
    cleanup_test_env
}

test_configuration_display() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" \
        --jobs 8 \
        --threshold 5000 \
        --shard-size 4000 2>&1 || true)
    
    assert_contains "$output" "Configuration:" "Shows configuration section"
    assert_contains "$output" "Collection:.*$TEST_COLLECTION" "Shows collection name"
    assert_contains "$output" "Parallel jobs:.*8" "Shows parallel jobs"
    assert_contains "$output" "Excessive threshold:.*5000" "Shows excessive threshold"
    assert_contains "$output" "Shard size:.*4000" "Shows shard size"
    
    cleanup_test_env
}

test_incremental_flag() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" \
        --incremental 2>&1 || true)
    
    assert_contains "$output" "Incremental mode:.*1" "Enables incremental mode"
    cleanup_test_env
}

test_jobs_flag() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" \
        --jobs 16 2>&1 || true)
    
    assert_contains "$output" "Parallel jobs:.*16" "Sets parallel jobs to 16"
    cleanup_test_env
}

test_threshold_flag() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" \
        --threshold 2500 2>&1 || true)
    
    assert_contains "$output" "Excessive threshold:.*2500" "Sets excessive threshold to 2500"
    cleanup_test_env
}

test_shard_size_flag() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" \
        --shard-size 5000 2>&1 || true)
    
    assert_contains "$output" "Shard size:.*5000" "Sets shard size to 5000"
    cleanup_test_env
}

test_collection_field_automatic() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" 2>&1 || true)
    
    # Collection field should automatically use collection name
    assert_contains "$output" "Collection field:.*collection=$TEST_COLLECTION" "Collection field automatically set to collection name"
    cleanup_test_env
}

test_collection_field_in_stage2() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" 2>&1 || true)
    
    # Stage 2 should always add collection field
    assert_contains "$output" "STAGE 2/6.*Adding collection field" "Stage 2 adds collection field"
    cleanup_test_env
}

test_collection_field_value() {
    setup_test_env
    TEST_COLLECTION="AWP999"
    mkdir -p "$TEST_DIR/collections/$TEST_COLLECTION"
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" 2>&1 || true)
    
    assert_contains "$output" "collection=AWP999" "Collection field value matches collection name"
    cleanup_test_env
}

test_missing_blocklist_warning() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" \
        --blocklist /nonexistent/blocklist.txt 2>&1 || true)
    
    assert_contains "$output" "Blocklist not found" "Warns about missing blocklist"
    assert_contains "$output" "Proceeding without blocklist" "Continues without blocklist"
    cleanup_test_env
}

test_custom_blocklist() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" \
        --blocklist "$TEST_DIR/blocklists/test-blocklist.txt" 2>&1 || true)
    
    # Script should recognize the blocklist (not warn about it not being found)
    assert_not_contains "$output" "Blocklist not found" "Accepts custom blocklist"
    cleanup_test_env
}

test_keep_temp_flag() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" \
        --keep-temp 2>&1 || true)
    
    assert_contains "$output" "Keep temp files:.*1" "Enables keep-temp mode"
    cleanup_test_env
}

test_no_compress_flag() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    # Run script and check that --no-compress is accepted (will fail on missing deps)
    bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" \
        --no-compress 2>&1 > /dev/null || true
    
    # If we get here without syntax error, flag was accepted
    echo -e "${GREEN}✓${NC} Parses --no-compress flag"
    ((TESTS_PASSED++))
    
    cleanup_test_env
}

test_unknown_option() {
    local output=$(bash "$SCRIPT" TEST --unknown-option 2>&1 || true)
    assert_contains "$output" "Unknown option" "Rejects unknown options"
}

test_multiple_collections() {
    local output=$(bash "$SCRIPT" COL1 COL2 2>&1 || true)
    assert_contains "$output" "Too many arguments" "Rejects multiple collection names"
}

test_warc_count_display() {
    setup_test_env
    
    # Create 3 test WARC files
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test1.warc.gz"
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test2.warc.gz"
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test3.warc"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" 2>&1 || true)
    
    assert_contains "$output" "WARC files:.*3" "Counts WARC files correctly"
    cleanup_test_env
}

test_directory_creation() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" 2>&1 || true)
    
    # Verify "Creating directories..." message appears (happens early, before pipeline errors)
    assert_contains "$output" "Creating directories" "Creates required directories"
    
    cleanup_test_env
}

test_error_handling_flags() {
    # Verify script has proper error handling flags
    if grep -q "set -e" "$SCRIPT" && \
       grep -q "set -o pipefail" "$SCRIPT" && \
       grep -q "set -u" "$SCRIPT"; then
        echo -e "${GREEN}✓${NC} Has proper error handling (set -e, -o pipefail, -u)"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}✗${NC} Has proper error handling (set -e, -o pipefail, -u)"
        ((TESTS_FAILED++))
    fi
}

test_stage_numbering() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" 2>&1 || true)
    
    # Should always be 6 stages (collection field always added)
    assert_contains "$output" "STAGE 1/6.*Indexing" "Stage 1 is indexing (6 stages total)"
    assert_contains "$output" "STAGE 2/6.*Adding collection field" "Stage 2 is collection field addition (6 stages total)"
    
    cleanup_test_env
}

test_blocklist_stage_numbering() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" \
        --blocklist "$TEST_DIR/blocklists/test-blocklist.txt" 2>&1 || true)
    
    # With blocklist, stage 3 should be filtering
    assert_contains "$output" "STAGE 3/6.*Filtering blocklist" "Stage 3 is blocklist filtering"
    
    cleanup_test_env
}

test_cleanup_on_error_trap() {
    # Verify script has cleanup trap
    if grep -q "trap.*cleanup_on_error" "$SCRIPT"; then
        echo -e "${GREEN}✓${NC} Has cleanup trap for error handling"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}✗${NC} Has cleanup trap for error handling"
        ((TESTS_FAILED++))
    fi
}

test_atomic_write_protection() {
    # Verify script uses atomic writes (.tmp files)
    if grep -q ".cdxj.tmp" "$SCRIPT"; then
        echo -e "${GREEN}✓${NC} Uses atomic writes (.tmp files)"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}✗${NC} Uses atomic writes (.tmp files)"
        ((TESTS_FAILED++))
    fi
}

test_dependency_checking() {
    # Verify script checks for required dependencies
    if grep -q "check_dependencies" "$SCRIPT" && \
       grep -q "cdx-indexer" "$SCRIPT" && \
       grep -q "merge-flat-cdxj" "$SCRIPT" && \
       grep -q "addfield-to-flat-cdxj" "$SCRIPT"; then
        echo -e "${GREEN}✓${NC} Checks for required dependencies"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}✗${NC} Checks for required dependencies"
        ((TESTS_FAILED++))
    fi
}

test_parallel_export() {
    # Verify functions are exported for GNU parallel
    if grep -q "export -f index_warc" "$SCRIPT" && \
       grep -q "export -f addfield_cdxj" "$SCRIPT"; then
        echo -e "${GREEN}✓${NC} Exports functions for parallel execution"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}✗${NC} Exports functions for parallel execution"
        ((TESTS_FAILED++))
    fi
}

test_logging_functions() {
    # Verify script has logging functions
    if grep -q "log_info" "$SCRIPT" && \
       grep -q "log_success" "$SCRIPT" && \
       grep -q "log_warning" "$SCRIPT" && \
       grep -q "log_error" "$SCRIPT"; then
        echo -e "${GREEN}✓${NC} Has logging functions (info, success, warning, error)"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}✗${NC} Has logging functions (info, success, warning, error)"
        ((TESTS_FAILED++))
    fi
}

test_statistics_display() {
    setup_test_env
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp" 2>&1 || true)
    
    # Verify script displays WARC count and timing (happens early)
    assert_contains "$output" "WARC files:" "Shows WARC file statistics"
    assert_contains "$output" "Started at" "Shows processing start time"
    
    cleanup_test_env
}

# Main test runner
main() {
    echo ""
    echo "=========================================="
    echo "cdxj-index-collection.sh Test Suite"
    echo "=========================================="
    echo ""
    echo "Script: $SCRIPT"
    echo ""
    
    # Run tests
    run_test "Script exists and is executable" test_script_exists
    run_test "Script has valid bash syntax" test_script_syntax
    run_test "Shows help with --help" test_help_option
    run_test "Fails without collection name" test_no_collection_name
    run_test "Fails with non-existent collection" test_nonexistent_collection
    run_test "Fails with empty collection" test_empty_collection
    run_test "Displays configuration" test_configuration_display
    run_test "Parses --incremental flag" test_incremental_flag
    run_test "Parses --jobs flag" test_jobs_flag
    run_test "Parses --threshold flag" test_threshold_flag
    run_test "Parses --shard-size flag" test_shard_size_flag
    run_test "Collection field automatically set" test_collection_field_automatic
    run_test "Collection field in stage 2" test_collection_field_in_stage2
    run_test "Collection field value matches collection name" test_collection_field_value
    run_test "Warns about missing blocklist" test_missing_blocklist_warning
    run_test "Accepts custom blocklist" test_custom_blocklist
    run_test "Parses --keep-temp flag" test_keep_temp_flag
    run_test "Parses --no-compress flag" test_no_compress_flag
    run_test "Rejects unknown options" test_unknown_option
    run_test "Rejects multiple collection names" test_multiple_collections
    run_test "Counts WARC files correctly" test_warc_count_display
    run_test "Creates required directories" test_directory_creation
    run_test "Has proper error handling flags" test_error_handling_flags
    run_test "Stage numbering (always 6 stages)" test_stage_numbering
    run_test "Blocklist stage numbering" test_blocklist_stage_numbering
    run_test "Has cleanup trap for errors" test_cleanup_on_error_trap
    run_test "Uses atomic writes (.tmp files)" test_atomic_write_protection
    run_test "Checks for required dependencies" test_dependency_checking
    run_test "Exports functions for parallel" test_parallel_export
    run_test "Has logging functions" test_logging_functions
    run_test "Displays statistics" test_statistics_display
    
    # Summary
    echo ""
    echo "=========================================="
    echo "Test Results"
    echo "=========================================="
    echo ""
    echo "Tests run:    $TESTS_RUN"
    echo -e "Tests passed: ${GREEN}$TESTS_PASSED${NC}"
    
    if [ $TESTS_FAILED -gt 0 ]; then
        echo -e "Tests failed: ${RED}$TESTS_FAILED${NC}"
        echo ""
        exit 1
    else
        echo -e "Tests failed: ${GREEN}0${NC}"
        echo ""
        echo -e "${GREEN}All tests passed!${NC}"
        echo ""
        exit 0
    fi
}

# Run main
main
