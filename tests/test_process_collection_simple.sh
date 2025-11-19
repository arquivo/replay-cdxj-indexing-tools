#!/bin/bash
#
# Simple test suite for process-collection.sh
# Pure bash implementation - no external dependencies needed
#
# Usage:
#   bash tests/test_process_collection_simple.sh
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

test_help_option() {
    local output=$(bash "$SCRIPT" --help 2>&1 || true)
    assert_contains "$output" "Usage:" "Shows usage with --help"
    assert_contains "$output" "incremental" "Help includes --incremental option"
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

test_incremental_flag() {
    # Just verify the flag is accepted without error
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" --incremental --help 2>&1 || true)
    # If help is shown, the flag was parsed successfully
    assert_contains "$output" "Usage:" "Parses --incremental flag"
}

test_jobs_flag() {
    # Just verify the flag is accepted without error
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" --jobs 4 --help 2>&1 || true)
    assert_contains "$output" "Usage:" "Parses --jobs flag"
}

test_threshold_flag() {
    # Just verify the flag is accepted without error
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" --threshold 5000 --help 2>&1 || true)
    assert_contains "$output" "Usage:" "Parses --threshold flag"
}

test_shard_size_flag() {
    # Just verify the flag is accepted without error
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" --shard-size 5000 --help 2>&1 || true)
    assert_contains "$output" "Usage:" "Parses --shard-size flag"
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
    
    # Script should exit due to missing dependencies, but should recognize the blocklist
    # The fact that it doesn't warn "Blocklist not found" means it was accepted
    if echo "$output" | grep -q "Blocklist not found"; then
        echo -e "${RED}✗${NC} Accepts custom blocklist"
        ((TESTS_FAILED++))
        return 1
    else
        echo -e "${GREEN}✓${NC} Accepts custom blocklist"
        ((TESTS_PASSED++))
        return 0
    fi
    cleanup_test_env
}

test_keep_temp_flag() {
    # Just verify the flag is accepted without error
    local output=$(bash "$SCRIPT" "$TEST_COLLECTION" --keep-temp --help 2>&1 || true)
    assert_contains "$output" "Usage:" "Parses --keep-temp flag"
}

test_script_syntax() {
    bash -n "$SCRIPT"
    assert_success "Script has valid bash syntax"
}

test_unknown_option() {
    local output=$(bash "$SCRIPT" TEST --unknown-option 2>&1 || true)
    assert_contains "$output" "Unknown option" "Rejects unknown options"
}

test_no_confirmation_prompt() {
    # Verify the script runs without interactive prompts
    # (confirmation prompt was removed for automated execution)
    if ! grep -q "Continue?" "$SCRIPT"; then
        echo -e "${GREEN}✓${NC} No interactive confirmation prompt (for automation)"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗${NC} No interactive confirmation prompt (for automation)"
        ((TESTS_FAILED++))
        return 1
    fi
}

# Main test runner
main() {
    echo ""
    echo "=========================================="
    echo "Process Collection Script Test Suite"
    echo "=========================================="
    echo ""
    echo "Script: $SCRIPT"
    echo ""
    
    # Run tests
    run_test "Script exists and is executable" test_script_exists
    run_test "Script has valid syntax" test_script_syntax
    run_test "Shows help with --help" test_help_option
    run_test "Fails without collection name" test_no_collection_name
    run_test "Fails with non-existent collection" test_nonexistent_collection
    run_test "Fails with empty collection" test_empty_collection
    run_test "Parses --incremental flag" test_incremental_flag
    run_test "Parses --jobs flag" test_jobs_flag
    run_test "Parses --threshold flag" test_threshold_flag
    run_test "Parses --shard-size flag" test_shard_size_flag
    run_test "Warns about missing blocklist" test_missing_blocklist_warning
    run_test "Accepts custom blocklist" test_custom_blocklist
    run_test "Parses --keep-temp flag" test_keep_temp_flag
    run_test "Rejects unknown options" test_unknown_option
    run_test "No interactive confirmation prompt" test_no_confirmation_prompt
    
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
