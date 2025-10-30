#!/bin/bash
# Avellaneda-Stoikov Scalping Bot - Quick Launcher
# Starts all 9 microservices in the background with tmux

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$PROJECT_DIR/.venv"
SESSION_NAME="scalping-bot"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

echo "=================================="
echo -e "${BOLD}${BLUE}Avellaneda-Stoikov Scalping Bot${NC}"
echo -e "${BOLD}Starting Microservices${NC}"
echo "=================================="
echo ""

# Check if virtual environment exists
if [ ! -d "$VENV" ]; then
    echo -e "${RED}Error: Virtual environment not found at $VENV${NC}"
    echo "Please run: python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Check if tmux is installed
if ! command -v tmux &> /dev/null; then
    echo -e "${YELLOW}Warning: tmux not found. Using background processes instead.${NC}"
    echo ""

    # Activate venv
    source "$VENV/bin/activate"

    cd "$PROJECT_DIR"

    # Start services in background
    echo -e "${BOLD}Starting services in background...${NC}"
    echo ""

    python -m services.marketdata_gw.main > /tmp/scalping-bot-marketdata_gw.log 2>&1 &
    echo -e "${GREEN}✓${NC} marketdata_gw (PID: $!)"

    python -m services.features_svc.main > /tmp/scalping-bot-features_svc.log 2>&1 &
    echo -e "${GREEN}✓${NC} features_svc (PID: $!)"

    python -m services.volflow_estimator.main > /tmp/scalping-bot-volflow_estimator.log 2>&1 &
    echo -e "${GREEN}✓${NC} volflow_estimator (PID: $!)"

    python -m services.as_engine.main > /tmp/scalping-bot-as_engine.log 2>&1 &
    echo -e "${GREEN}✓${NC} as_engine (PID: $!)"

    python -m services.order_router.main > /tmp/scalping-bot-order_router.log 2>&1 &
    echo -e "${GREEN}✓${NC} order_router (PID: $!)"

    python -m services.inventory_svc.main > /tmp/scalping-bot-inventory_svc.log 2>&1 &
    echo -e "${GREEN}✓${NC} inventory_svc (PID: $!)"

    python -m services.risk_manager.main > /tmp/scalping-bot-risk_manager.log 2>&1 &
    echo -e "${GREEN}✓${NC} risk_manager (PID: $!)"

    python -m services.metrics_svc.main > /tmp/scalping-bot-metrics_svc.log 2>&1 &
    echo -e "${GREEN}✓${NC} metrics_svc (PID: $!)"

    python -m services.sim_backtest.main > /tmp/scalping-bot-sim_backtest.log 2>&1 &
    echo -e "${GREEN}✓${NC} sim_backtest (PID: $!)"

    echo ""
    echo -e "${GREEN}${BOLD}✓ All services started!${NC}"
    echo ""
    echo -e "${BOLD}Log Files:${NC}"
    echo "  tail -f /tmp/scalping-bot-*.log"
    echo ""
    echo -e "${BOLD}To stop all services:${NC}"
    echo "  pkill -f 'services\\..*\\.main'"
    echo ""
    exit 0
fi

# Kill existing session if it exists
if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
    echo -e "${YELLOW}Terminating existing session...${NC}"
    tmux kill-session -t "$SESSION_NAME"
    sleep 1
fi

# Create new tmux session
echo -e "${BOLD}Creating tmux session: ${BLUE}$SESSION_NAME${NC}"
echo ""

tmux new-session -d -s "$SESSION_NAME" -x 200 -y 50

# Source virtual environment in the session
tmux send-keys -t "$SESSION_NAME" "cd '$PROJECT_DIR' && source '$VENV/bin/activate'" C-m
sleep 1

# List of services
declare -a SERVICES=(
    "marketdata_gw:8001:Market Data Gateway"
    "features_svc:8002:Features Service"
    "volflow_estimator:8003:Volatility/Flow Estimator"
    "as_engine:8004:AS Strategy Engine"
    "order_router:8005:Order Router"
    "inventory_svc:8006:Inventory Service"
    "risk_manager:8007:Risk Manager"
    "metrics_svc:8008:Metrics Service"
    "sim_backtest:8009:Simulation/Backtest"
)

# Create windows for each service
for i in "${!SERVICES[@]}"; do
    IFS=':' read -r SERVICE PORT DESC <<< "${SERVICES[$i]}"

    if [ $i -eq 0 ]; then
        # Rename first window
        tmux rename-window -t "$SESSION_NAME:0" "$SERVICE"
        tmux send-keys -t "$SESSION_NAME:0" "python -m services.$SERVICE.main" C-m
    else
        # Create new window
        tmux new-window -t "$SESSION_NAME" -n "$SERVICE"
        tmux send-keys -t "$SESSION_NAME:$i" "cd '$PROJECT_DIR' && source '$VENV/bin/activate' && python -m services.$SERVICE.main" C-m
    fi

    echo -e "${GREEN}✓${NC} $SERVICE (port $PORT) - $DESC"
done

echo ""
echo -e "${GREEN}${BOLD}✓ All services started in tmux session!${NC}"
echo ""
echo -e "${BOLD}Available Endpoints:${NC}"
echo "  Prometheus:  http://localhost:9090"
echo "  Grafana:     http://localhost:3000 (admin/admin)"
echo ""
echo -e "${BOLD}tmux Commands:${NC}"
echo "  Attach to session:    ${BLUE}tmux attach -t $SESSION_NAME${NC}"
echo "  List windows:         ${BLUE}tmux list-windows -t $SESSION_NAME${NC}"
echo "  Select window:        ${BLUE}tmux select-window -t $SESSION_NAME:marketdata_gw${NC}"
echo "  Kill session:         ${BLUE}tmux kill-session -t $SESSION_NAME${NC}"
echo "  Kill all sessions:    ${BLUE}tmux kill-server${NC}"
echo ""
echo -e "${BOLD}Quick View:${NC}"
echo "  ${BLUE}tmux attach -t $SESSION_NAME${NC}"
echo ""
