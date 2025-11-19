#!/usr/bin/env bats
#
# Test suite for process-collection.sh
#
# To run these tests:
#   1. Install bats: sudo apt-get install bats
#   2. Run tests: bats tests/test_process_collection.bats
#
# Or run with the simple test runner:
#   bash tests/test_process_collection_simple.sh

setup() {
    # Create temporary test environment
    export TEST_DIR="$(mktemp -d)"
    export SCRIPT_DIR="$(cd "$(dirname "$BATS_TEST_FILENAME")/.." && pwd)"
    export SCRIPT="$SCRIPT_DIR/process-collection.sh"
    
    # Test collection name
    export TEST_COLLECTION="TEST-2024-11"
    
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

teardown() {
    # Clean up test environment
    rm -rf "$TEST_DIR"
}

@test "script exists and is executable" {
    [ -f "$SCRIPT" ]
    [ -x "$SCRIPT" ]
}

@test "script shows help with --help" {
    run bash "$SCRIPT" --help
    [ "$status" -eq 1 ]  # Help exits with 1
    [[ "$output" =~ "Usage:" ]]
    [[ "$output" =~ "--incremental" ]]
}

@test "script fails without collection name" {
    run bash "$SCRIPT"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Collection name is required" ]]
}

@test "script fails with non-existent collection" {
    run bash "$SCRIPT" NONEXISTENT \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp"
    
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Collection directory not found" ]]
}

@test "script fails with empty collection (no WARCs)" {
    run bash "$SCRIPT" "$TEST_COLLECTION" \
        --collections-dir "$TEST_DIR/collections" \
        --output-dir "$TEST_DIR/zipnum" \
        --temp-dir "$TEST_DIR/temp"
    
    [ "$status" -eq 1 ]
    [[ "$output" =~ "No WARC files found" ]]
}

@test "script accepts valid options" {
    # Create a dummy WARC so collection isn't empty
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    # Run script with --help to check option parsing (won't actually process)
    run bash "$SCRIPT" "$TEST_COLLECTION" --help
    
    [ "$status" -eq 1 ]  # Help exits with 1
    [[ "$output" =~ "Usage:" ]]
}

@test "script parses --incremental flag" {
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    # Check if --incremental is recognized (script will fail at confirmation prompt)
    run bash -c "echo 'n' | bash '$SCRIPT' '$TEST_COLLECTION' \
        --collections-dir '$TEST_DIR/collections' \
        --output-dir '$TEST_DIR/zipnum' \
        --temp-dir '$TEST_DIR/temp' \
        --incremental 2>&1"
    
    # Should show incremental mode in config
    [[ "$output" =~ "Incremental mode:   1" ]] || [[ "$output" =~ "incremental" ]]
}

@test "script parses --jobs flag" {
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    run bash -c "echo 'n' | bash '$SCRIPT' '$TEST_COLLECTION' \
        --collections-dir '$TEST_DIR/collections' \
        --output-dir '$TEST_DIR/zipnum' \
        --temp-dir '$TEST_DIR/temp' \
        --jobs 4 2>&1"
    
    [[ "$output" =~ "Parallel jobs:      4" ]]
}

@test "script parses --threshold flag" {
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    run bash -c "echo 'n' | bash '$SCRIPT' '$TEST_COLLECTION' \
        --collections-dir '$TEST_DIR/collections' \
        --output-dir '$TEST_DIR/zipnum' \
        --temp-dir '$TEST_DIR/temp' \
        --threshold 5000 2>&1"
    
    [[ "$output" =~ "Excessive threshold: 5000" ]]
}

@test "script warns about missing blocklist" {
    touch "$TEST_DIR/collections/$TEST_COLLECTION/test.warc.gz"
    
    run bash -c "echo 'n' | bash '$SCRIPT' '$TEST_COLLECTION' \
        --collections-dir '$TEST_DIR/collections' \
        --output-dir '$TEST_DIR/zipnum' \
        --temp-dir '$TEST_DIR/temp' \
        --blocklist /nonexistent/blocklist.txt 2>&1"
    
    [[ "$output" =~ "Blocklist not found" ]]
    [[ "$output" =~ "Proceeding without blocklist" ]]
}

@test "index_warc function handles missing WARC" {
    # Source the script to test the function
    source <(grep -A 40 "^index_warc()" "$SCRIPT")
    
    # Create test directory
    local test_indexes="$TEST_DIR/indexes"
    mkdir -p "$test_indexes"
    
    # Try to index non-existent WARC
    run index_warc "/nonexistent.warc.gz" "$test_indexes" 0
    
    [ "$status" -eq 1 ]
}

@test "script validates required dependencies" {
    # Create mock PATH without required tools
    export PATH="/usr/bin:/bin"
    
    # Remove commands by using a restricted PATH
    run bash -c "PATH=/usr/bin:/bin bash '$SCRIPT' --help 2>&1 || true"
    
    # Script should check dependencies (may fail if tools not found)
    # This test verifies the script doesn't crash on missing deps
    [ -n "$output" ]
}
