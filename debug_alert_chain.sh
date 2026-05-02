#!/bin/bash
# =====================================================================
# debug_alert_chain.sh — Debug toàn chain alert end-to-end
# =====================================================================
# Cách dùng:
#   1. Đảm bảo postgres-source đã được tắt (docker stop postgres-source)
#   2. Đợi >= 90s
#   3. Chạy: bash debug_alert_chain.sh
# =====================================================================

set +e  # KHÔNG exit khi error - tiếp tục check

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

ok()    { echo -e "  ${GREEN}✓${NC} $1"; }
warn()  { echo -e "  ${YELLOW}⚠${NC} $1"; }
fail()  { echo -e "  ${RED}✗${NC} $1"; }
hr()    { echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

hr
echo -e "${BLUE}KIỂM TRA 1: Container đang chạy${NC}"
hr
for svc in prometheus alertmanager alertmanager-discord postgres-exporter-source; do
    if docker ps --format '{{.Names}}' | grep -q "^${svc}$"; then
        ok "$svc UP"
    else
        fail "$svc KHÔNG chạy"
    fi
done

hr
echo -e "${BLUE}KIỂM TRA 2: postgres-source phải đang TẮT${NC}"
hr
if docker ps --format '{{.Names}}' | grep -q "^postgres-source$"; then
    fail "postgres-source vẫn ĐANG CHẠY → alert sẽ không fire"
    echo "    → Chạy: docker stop postgres-source"
else
    ok "postgres-source đã stop (đúng)"
fi

hr
echo -e "${BLUE}KIỂM TRA 3: DISCORD_WEBHOOK env trong container${NC}"
hr
WEBHOOK=$(docker exec alertmanager-discord env 2>/dev/null | grep '^DISCORD_WEBHOOK=' | cut -d'=' -f2-)
if [ -z "$WEBHOOK" ]; then
    fail "DISCORD_WEBHOOK env TRỐNG trong container!"
    echo "    → File .env có DISCORD_WEBHOOK_URL chưa? cat .env | grep DISCORD"
    echo "    → Sau khi sửa .env: docker compose down && docker compose up -d"
elif [[ "$WEBHOOK" != https://discord.com/api/webhooks/* ]]; then
    fail "DISCORD_WEBHOOK URL trông không đúng format: ${WEBHOOK:0:50}..."
else
    ok "DISCORD_WEBHOOK đã set: ${WEBHOOK:0:60}..."
fi

hr
echo -e "${BLUE}KIỂM TRA 4: Test webhook trực tiếp (bypass Alertmanager)${NC}"
hr
if [ -n "$WEBHOOK" ]; then
    HTTP_CODE=$(curl -s -o /tmp/discord_resp -w "%{http_code}" \
                -H "Content-Type: application/json" \
                -d '{"content":"🧪 Test từ debug script - bỏ qua message này"}' \
                "$WEBHOOK")
    if [ "$HTTP_CODE" = "204" ]; then
        ok "Webhook OK (HTTP 204) → check Discord phải thấy message test"
    else
        fail "Webhook fail HTTP $HTTP_CODE → response: $(cat /tmp/discord_resp)"
        echo "    → Webhook URL có thể sai/expired. Tạo webhook mới."
    fi
else
    warn "Bỏ qua vì WEBHOOK không có"
fi

hr
echo -e "${BLUE}KIỂM TRA 5: Prometheus thấy postgres_source DOWN?${NC}"
hr
UP_VALUE=$(curl -s 'http://localhost:9090/api/v1/query?query=up{job="postgres_source"}' \
           | python3 -c "import json,sys; r=json.load(sys.stdin); print(r['data']['result'][0]['value'][1] if r['data']['result'] else 'NO_DATA')" 2>/dev/null)
if [ "$UP_VALUE" = "0" ]; then
    ok "Prometheus thấy postgres_source DOWN (up=0) — đúng"
elif [ "$UP_VALUE" = "1" ]; then
    fail "Prometheus thấy postgres_source UP (up=1)"
    echo "    → postgres-exporter-source vẫn kết nối được? Check: docker logs postgres-exporter-source --tail 20"
elif [ "$UP_VALUE" = "NO_DATA" ]; then
    fail "Prometheus chưa scrape job postgres_source"
    echo "    → Check: curl http://localhost:9090/api/v1/targets"
else
    warn "Up value: $UP_VALUE"
fi

hr
echo -e "${BLUE}KIỂM TRA 6: Alert PostgreSQLSourceDown trong Prometheus${NC}"
hr
ALERT_STATE=$(curl -s http://localhost:9090/api/v1/alerts \
              | python3 -c "
import json,sys
data=json.load(sys.stdin)
for a in data.get('data',{}).get('alerts',[]):
    if a['labels'].get('alertname')=='PostgreSQLSourceDown':
        print(a['state'])
        break
else:
    print('NOT_FOUND')
" 2>/dev/null)
if [ "$ALERT_STATE" = "firing" ]; then
    ok "Alert PostgreSQLSourceDown đang FIRING — đúng"
elif [ "$ALERT_STATE" = "pending" ]; then
    warn "Alert đang PENDING — đợi thêm 60s rồi chạy lại script"
elif [ "$ALERT_STATE" = "NOT_FOUND" ]; then
    fail "Alert chưa fire. postgres-source mới tắt? Đợi >= 90s"
fi

hr
echo -e "${BLUE}KIỂM TRA 7: Alertmanager nhận alert?${NC}"
hr
AM_ALERT=$(curl -s 'http://localhost:9093/api/v2/alerts' \
           | python3 -c "
import json,sys
data=json.load(sys.stdin)
for a in data:
    if a['labels'].get('alertname')=='PostgreSQLSourceDown':
        print(a['status']['state'])
        break
else:
    print('NOT_FOUND')
" 2>/dev/null)
if [ "$AM_ALERT" = "active" ]; then
    ok "Alertmanager đã nhận alert (state=active)"
elif [ "$AM_ALERT" = "NOT_FOUND" ]; then
    fail "Alertmanager CHƯA nhận alert"
    echo "    → Prometheus chưa đẩy được. Check log:"
    echo "      docker logs prometheus 2>&1 | grep -i alertmanager | tail -5"
fi

hr
echo -e "${BLUE}KIỂM TRA 8: Alertmanager có gửi notify không?${NC}"
hr
NOTIFY_LOGS=$(docker logs alertmanager 2>&1 | grep -iE "notify|err" | tail -10)
if [ -z "$NOTIFY_LOGS" ]; then
    warn "Không có log notify từ Alertmanager"
else
    echo "$NOTIFY_LOGS"
fi

hr
echo -e "${BLUE}KIỂM TRA 9: Log của alertmanager-discord proxy${NC}"
hr
DISCORD_LOGS=$(docker logs alertmanager-discord 2>&1 | tail -20)
if [ -z "$DISCORD_LOGS" ]; then
    fail "Container alertmanager-discord không có log nào"
else
    echo "$DISCORD_LOGS"
fi

hr
echo -e "${BLUE}TÓM TẮT${NC}"
hr
echo "Nếu KIỂM TRA 4 fail → webhook URL sai → tạo webhook mới trên Discord"
echo "Nếu KIỂM TRA 4 OK nhưng KIỂM TRA 9 lỗi → image alertmanager-discord vấn đề"
echo "  → Thử đổi sang: rogerrum/alertmanager-discord:1.6.1"
echo "Nếu KIỂM TRA 7 fail → Prometheus không kết nối được Alertmanager"
echo ""
