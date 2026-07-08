# test_queries.sh # 01:13:21 06.06.2026
# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

QUERY_HOST="localhost"
QUERY_PORT="9999"

# Default mint value
DEFAULT_MINT="Bji9FW1DKL6DEGwuFJCNEkKqYCRQaK2pDn1kk5k9pump"
MINT="${1:-$DEFAULT_MINT}"

# Function to send query and print result with timeout
send_query() {
    local query="$1"
    local description="$2"
    local timeout="${3:-5}"  # Default timeout 5 seconds
    
    echo -e "${YELLOW}▶ $description${NC}"
    echo -e "${GREEN}Query: $query${NC}"
    echo -e "${NC}Response:${NC}"
    
    # Send query with timeout and capture exit code
    if echo "$query" | timeout "$timeout" nc "$QUERY_HOST" "$QUERY_PORT" | jq '.' 2>/dev/null; then
        echo -e "${GREEN}✓ Query completed successfully${NC}"
    else
        local exit_code=$?
        if [ $exit_code -eq 124 ]; then
            echo -e "${RED}✗ Query timed out after ${timeout} seconds${NC}"
        else
            echo -e "${RED}✗ Query failed with exit code: $exit_code${NC}"
        fi
    fi
    
    echo "----------------------------------------"
    echo ""
}

# Alternative: Using a temporary file with timeout
send_query_robust() {
    local query="$1"
    local description="$2"
    local timeout="${3:-5}"
    local temp_file=$(mktemp)
    
    echo -e "${YELLOW}▶ $description${NC}"
    echo -e "${GREEN}Query: $query${NC}"
    echo -e "${NC}Response:${NC}"
    
    # Run nc with timeout and capture output
    echo "$query" | timeout "$timeout" nc "$QUERY_HOST" "$QUERY_PORT" > "$temp_file" 2>/dev/null
    
    if [ $? -eq 124 ]; then
        echo -e "${RED}✗ Query timed out after ${timeout} seconds${NC}"
    elif [ -s "$temp_file" ]; then
        # Try to parse as JSON if possible
        if jq '.' "$temp_file" 2>/dev/null; then
            echo -e "${GREEN}✓ Query completed successfully${NC}"
        else
            echo -e "${YELLOW}⚠ Non-JSON response:${NC}"
            cat "$temp_file"
            echo ""
        fi
    else
        echo -e "${RED}✗ No response received${NC}"
    fi
    
    rm -f "$temp_file"
    echo "----------------------------------------"
    echo ""
}

# Function to check if server is available
check_server() {
    echo -e "${YELLOW}Checking server availability...${NC}"
    if echo "HELP" | timeout 2 nc "$QUERY_HOST" "$QUERY_PORT" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Server is reachable${NC}\n"
        return 0
    else
        echo -e "${RED}✗ Server is not reachable on $QUERY_HOST:$QUERY_PORT${NC}"
        echo -e "${YELLOW}Continuing with tests anyway...${NC}\n"
        return 1
    fi
}

# Show usage if help requested
if [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]]; then
    echo -e "${GREEN}Usage: $0 [mint_address]${NC}"
    echo -e "Default mint: $DEFAULT_MINT"
    exit 0
fi

# Check server before running tests
check_server

# Run all tests regardless of individual failures
echo -e "${GREEN}Starting tests with mint: $MINT${NC}\n"

# Test 1: Get server help
send_query "HELP" "Getting server help" 3

# Test 2: Get database stats
send_query "GET stats" "Database statistics" 5

# Test 3: Get messages for a Solana mint
send_query "GET mint $MINT" "Messages for Solana mint" 10

# Test 4: Get unique chats for a mint
send_query "GET mint $MINT --unique-chats" "Unique chats for Solana mint" 10

# Test 5: Get chat by ID
send_query "GET chat:id -1003568338417" "Chat info by ID" 5

# Test 6: Get chat by name
send_query "GET chat:name 777" "Chat info by name" 5

# Test 7: Cleanup old messages (dry run - 1 year old)
send_query "CLEANUP 31536000" "Cleanup messages older than 1 year" 30

echo -e "${GREEN}All tests completed!${NC}"