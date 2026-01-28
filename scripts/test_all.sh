#!/bin/bash
# Run all integration tests

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}🧪 Running Scorpius Integration Tests${NC}\n"

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    export DATABASE_URL="postgresql://postgres:postgres@localhost/scorpius_test"
    echo -e "${YELLOW}Using default DATABASE_URL: $DATABASE_URL${NC}\n"
fi

# Check database connection
echo -e "${YELLOW}Checking database connection...${NC}"
if ! psql "$DATABASE_URL" -c "SELECT 1" > /dev/null 2>&1; then
    echo -e "${RED}❌ Cannot connect to database at $DATABASE_URL${NC}"
    echo -e "${YELLOW}Please ensure PostgreSQL is running and DATABASE_URL is correct${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Database connection OK${NC}\n"

# Run ledger integration tests
echo -e "${YELLOW}📊 Running Ledger Integration Tests...${NC}"
if cargo test --package ledger --test integration_test; then
    echo -e "${GREEN}✅ Ledger tests passed${NC}\n"
else
    echo -e "${RED}❌ Ledger tests failed${NC}\n"
    exit 1
fi

# Run matching engine tests
echo -e "${YELLOW}⚡ Running Matching Engine Tests...${NC}"
if cargo test --package matching --test integration_test; then
    echo -e "${GREEN}✅ Matching engine tests passed${NC}\n"
else
    echo -e "${RED}❌ Matching engine tests failed${NC}\n"
    exit 1
fi

# Run unit tests
echo -e "${YELLOW}🔬 Running Unit Tests...${NC}"
if cargo test --lib; then
    echo -e "${GREEN}✅ Unit tests passed${NC}\n"
else
    echo -e "${RED}❌ Unit tests failed${NC}\n"
    exit 1
fi

echo -e "${GREEN}🎉 All tests passed!${NC}"
