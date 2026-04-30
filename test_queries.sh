#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

QUERY_HOST="localhost"
QUERY_PORT="9999"

# Function to send query and print result
send_query() {
    local query="$1"
    local description="$2"
    
    echo -e "${YELLOW}▶ $description${NC}"
    echo -e "${GREEN}Query: $query${NC}"
    echo -e "${NC}Response:${NC}"
    echo "$query" | nc "$QUERY_HOST" "$QUERY_PORT" | jq '.'
    echo "----------------------------------------"
    echo ""
}

# Test 1: Get server help
send_query "HELP" "Getting server help"

# Test 2: Get database stats
send_query "GET stats" "Database statistics"

# Test 3: Get messages for a Solana mint
send_query "GET mint Bji9FW1DKL6DEGwuFJCNEkKqYCRQaK2pDn1kk5k9pump" "Messages for Solana mint"

# Test 4: Get unique chats for a mint
send_query "GET mint Bji9FW1DKL6DEGwuFJCNEkKqYCRQaK2pDn1kk5k9pump --unique-chats" "Unique chats for Solana mint"

# Test 5: Get chat by ID
send_query "GET chat:id -1003568338417" "Chat info by ID"

# Test 6: Get chat by name
send_query "GET chat:name 777" "Chat info by name"

# Test 7: Cleanup old messages (dry run - 1 year old)
send_query "CLEANUP 31536000" "Cleanup messages older than 1 year"

echo -e "${GREEN}All tests completed!${NC}"