#!/bin/bash
set -e

# Air-gap: prevent HuggingFace Hub from phoning home (model is baked into image)
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1
export HF_HUB_DISABLE_TELEMETRY=1
export DO_NOT_TRACK=1

# MemoryError fix: cap native threadpools to prevent thread-explosion
# under x86 emulation (OrbStack/Rosetta2 on Apple Silicon).
: "${OMP_NUM_THREADS:=1}"
: "${OPENBLAS_NUM_THREADS:=1}"
: "${MKL_NUM_THREADS:=1}"
: "${VECLIB_MAXIMUM_THREADS:=1}"
: "${NUMEXPR_NUM_THREADS:=1}"
: "${TORCH_NUM_THREADS:=1}"
: "${TOKENIZERS_PARALLELISM:=false}"
export OMP_NUM_THREADS OPENBLAS_NUM_THREADS MKL_NUM_THREADS VECLIB_MAXIMUM_THREADS NUMEXPR_NUM_THREADS TORCH_NUM_THREADS TOKENIZERS_PARALLELISM

echo ""
echo "  ╔═══════════════════════════════════════════╗"
echo "  ║                                           ║"
echo "  ║       ⚡ vectorAIz v${VECTORAIZ_VERSION:-dev}               ║"
echo "  ║                                           ║"
echo "  ║   Mode: $(printf '%-34s' "${VECTORAIZ_MODE:-standalone}")║"
echo "  ║                                           ║"
echo "  ║   ➜  http://localhost                     ║"
echo "  ║                                           ║"
echo "  ╚═══════════════════════════════════════════╝"
echo ""

# Auto-generate HMAC secret if not provided
if [ -z "$VECTORAIZ_APIKEY_HMAC_SECRET" ]; then
    HMAC_FILE="/data/.vectoraiz_hmac_secret"
    if [ -f "$HMAC_FILE" ]; then
        export VECTORAIZ_APIKEY_HMAC_SECRET=$(cat "$HMAC_FILE")
        echo "[INFO] Using existing HMAC secret"
    else
        export VECTORAIZ_APIKEY_HMAC_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")
        echo "$VECTORAIZ_APIKEY_HMAC_SECRET" > "$HMAC_FILE"
        chmod 600 "$HMAC_FILE"
        echo "[INFO] Generated HMAC secret"
    fi
fi

# Auto-generate SECRET_KEY if not provided
if [ -z "$VECTORAIZ_SECRET_KEY" ]; then
    SECRET_FILE="/data/.vectoraiz_secret_key"
    if [ -f "$SECRET_FILE" ]; then
        export VECTORAIZ_SECRET_KEY=$(cat "$SECRET_FILE")
        echo "[INFO] Using existing encryption key"
    else
        export VECTORAIZ_SECRET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
        echo "$VECTORAIZ_SECRET_KEY" > "$SECRET_FILE"
        chmod 600 "$SECRET_FILE"
        echo "[INFO] Generated encryption key"
    fi
fi

# Run database migrations
echo "[INFO] Running database migrations..."
cd /app && python -m alembic upgrade head
echo "[INFO] Migrations complete"

# Co-Pilot requires single-worker mode (file lock enforced).
# Multi-worker support would require switching Co-Pilot to Redis pub/sub.
# nginx handles concurrent connections; uvicorn single worker handles async I/O.
VECTORAIZ_WORKERS=1

# Start uvicorn in background
echo "[INFO] Starting API server..."
uvicorn app.main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --workers ${VECTORAIZ_WORKERS} \
    --log-level ${VECTORAIZ_LOG_LEVEL:-info} &
UVICORN_PID=$!

# Start nginx in foreground mode (backgrounded for wait -n)
echo "[INFO] Starting web server..."
nginx -g 'daemon off;' &
NGINX_PID=$!

# If EITHER process exits, tear down so Docker restart policy can recover
wait -n
EXIT_CODE=$?
echo "[ERROR] Process exited (code=$EXIT_CODE) — shutting down container"
kill $UVICORN_PID $NGINX_PID 2>/dev/null
exit $EXIT_CODE
