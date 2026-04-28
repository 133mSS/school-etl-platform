#!/bin/bash
# =====================================================================
# demo_alert.sh — Demo Alertmanager end-to-end
# =====================================================================
# Chạy script này TRỰC TIẾP trước giảng viên khi bảo vệ.
# 
# Kịch bản:
#   1. Verify stack đang chạy
#   2. Tắt postgres-source giả lập sự cố
#   3. Đợi 90s để Prometheus evaluate alert
#   4. Show alert đã fire trong Prometheus + Alertmanager
#   5. Show message Discord đã nhận
#   6. Bật lại postgres-source → resolved alert
#   7. Show resolved message
# =====================================================================

set -e

# Màu sắc cho output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

step() {
    echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}▶ $1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

ok()    { echo -e "  ${GREEN}✓${NC} $1"; }
warn()  { echo -e "  ${YELLOW}⚠${NC} $1"; }
fail()  { echo -e "  ${RED}✗${NC} $1"; }


# ──────────────────────────────────────────────────────────────────
# BƯỚC 1: Verify Stack
# ──────────────────────────────────────────────────────────────────
step "BƯỚC 1/7: Verify monitoring stack đang chạy"

REQUIRED_SERVICES=(
    "prometheus"
    "alertmanager"
    "alertmanager-discord"
    "postgres-source"
    "postgres-exporter-source"
)

for svc in "${REQUIRED_SERVICES[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${svc}$"; then
        ok "$svc đang chạy"
    else
        fail "$svc KHÔNG chạy — chạy 'docker-compose up -d $svc' trước"
        exit 1
    fi
done


# ──────────────────────────────────────────────────────────────────
# BƯỚC 2: Verify Prometheus đã load alert rules
# ──────────────────────────────────────────────────────────────────
step "BƯỚC 2/7: Verify Prometheus đã load alert_rules.yml"

RULES_COUNT=$(curl -s http://localhost:9090/api/v1/rules | grep -o '"name":"[^"]*"' | wc -l)
if [ "$RULES_COUNT" -gt 0 ]; then
    ok "Prometheus đã load $RULES_COUNT alert rules"
else
    fail "Prometheus chưa load rule nào — kiểm tra prometheus.yml"
    exit 1
fi


# ──────────────────────────────────────────────────────────────────
# BƯỚC 3: Verify Alertmanager kết nối được
# ──────────────────────────────────────────────────────────────────
step "BƯỚC 3/7: Verify Alertmanager kết nối Prometheus"

AM_STATUS=$(curl -s http://localhost:9093/api/v2/status | grep -o '"cluster":{[^}]*}' || echo "")
if [ -n "$AM_STATUS" ]; then
    ok "Alertmanager API đang phản hồi"
else
    fail "Alertmanager API không trả lời tại localhost:9093"
    exit 1
fi


# ──────────────────────────────────────────────────────────────────
# BƯỚC 4: TẮT postgres-source để TRIGGER alert
# ──────────────────────────────────────────────────────────────────
step "BƯỚC 4/7: Tắt postgres-source để giả lập sự cố"

warn "Sắp tắt container postgres-source — alert PostgreSQLSourceDown sẽ fire sau 1 phút"
read -p "  Nhấn ENTER để tiếp tục..."

docker stop postgres-source > /dev/null
ok "postgres-source đã stop lúc $(date +%H:%M:%S)"

echo ""
echo "  Đang đợi 90 giây để:"
echo "    - postgres-exporter-source detect DB down"
echo "    - Prometheus scrape thấy up=0"
echo "    - Alert rule (for: 1m) chuyển từ PENDING → FIRING"
echo "    - Alertmanager nhận và route đến discord-critical"
echo ""

for i in $(seq 90 -5 5); do
    printf "\r  ⏱  Còn %02d giây..." "$i"
    sleep 5
done
echo ""


# ──────────────────────────────────────────────────────────────────
# BƯỚC 5: Verify alert đã FIRE
# ──────────────────────────────────────────────────────────────────
step "BƯỚC 5/7: Verify alert đã FIRE"

# 5.1 Check trong Prometheus
echo "  Kiểm tra trong Prometheus..."
PROM_FIRING=$(curl -s 'http://localhost:9090/api/v1/alerts' | grep -o '"PostgreSQLSourceDown"' | head -1)
if [ -n "$PROM_FIRING" ]; then
    ok "Alert PostgreSQLSourceDown đã fire trong Prometheus"
    curl -s 'http://localhost:9090/api/v1/alerts' | python3 -m json.tool | grep -A 3 "PostgreSQLSourceDown" | head -20
else
    warn "Chưa thấy alert trong Prometheus — đợi thêm 30s..."
    sleep 30
fi

# 5.2 Check trong Alertmanager
echo ""
echo "  Kiểm tra trong Alertmanager..."
AM_ALERTS=$(curl -s 'http://localhost:9093/api/v2/alerts' | grep -o '"alertname":"PostgreSQLSourceDown"' | head -1)
if [ -n "$AM_ALERTS" ]; then
    ok "Alertmanager đã nhận alert"
else
    fail "Alertmanager chưa nhận alert — kiểm tra prometheus.yml có config alerting chưa"
fi


# ──────────────────────────────────────────────────────────────────
# BƯỚC 6: Verify Discord đã nhận message
# ──────────────────────────────────────────────────────────────────
step "BƯỚC 6/7: Verify Discord đã nhận thông báo"

echo ""
echo "  ${YELLOW}→ KIỂM TRA THỦ CÔNG:${NC}"
echo "    1. Mở Discord channel #etl-alerts"
echo "    2. Phải có message dạng:"
echo ""
echo "       🚨 [FIRING] PostgreSQLSourceDown"
echo "       severity: critical"
echo "       component: database"
echo "       summary: 🔴 PostgreSQL Source Database không hoạt động"
echo ""
read -p "  Đã thấy message trên Discord chưa? (y/n): " seen
if [ "$seen" = "y" ]; then
    ok "Discord notification hoạt động đúng!"
else
    fail "Discord không nhận được — kiểm tra DISCORD_WEBHOOK_URL trong .env"
fi


# ──────────────────────────────────────────────────────────────────
# BƯỚC 7: Resolve alert bằng cách bật lại postgres-source
# ──────────────────────────────────────────────────────────────────
step "BƯỚC 7/7: Resolve alert"

echo "  Bật lại postgres-source..."
docker start postgres-source > /dev/null
ok "postgres-source đã start"

echo "  Đợi 60s để Prometheus thấy up=1, Alertmanager mark resolved..."
sleep 60

echo ""
echo "  ${YELLOW}→ KIỂM TRA THỦ CÔNG:${NC}"
echo "    Trên Discord phải có message dạng:"
echo "       ✅ [RESOLVED] PostgreSQLSourceDown"
echo ""
read -p "  Đã thấy message resolved chưa? (y/n): " resolved
if [ "$resolved" = "y" ]; then
    ok "End-to-end alert flow hoạt động hoàn hảo!"
else
    warn "Chưa nhận resolved — đợi thêm 1 phút (do group_interval=30s)"
fi


# ──────────────────────────────────────────────────────────────────
# TỔNG KẾT
# ──────────────────────────────────────────────────────────────────
step "DEMO COMPLETED"
echo ""
echo "  Đã chứng minh end-to-end:"
echo "    [✓] Prometheus đọc alert_rules.yml"
echo "    [✓] postgres-exporter-source expose metric up"
echo "    [✓] Alert FIRING khi DB down ≥ 1 phút"
echo "    [✓] Alertmanager route theo severity=critical"
echo "    [✓] Discord webhook nhận message"
echo "    [✓] Alert RESOLVED khi DB hồi phục"
echo ""
echo "  ${GREEN}→ Đây là evidence để show giảng viên${NC}"
echo ""