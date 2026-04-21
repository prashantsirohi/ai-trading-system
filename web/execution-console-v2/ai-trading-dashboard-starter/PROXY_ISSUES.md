# AI Trading Dashboard - API Connection Issue

## Problem

The React frontend (port 5173) cannot connect to the FastAPI backend (port 8090) to fetch real data.

### Symptoms
- Frontend loads but shows mock data
- Browser DevTools shows API calls failing (ERR_CONNECTION_REFUSED or CORS errors)
- Direct curl to backend works: `curl localhost:8090/api/execution/ranking` returns data
- curl to frontend proxy fails: `curl localhost:5173/api/execution/ranking` returns empty

### Root Cause
Vite's dev server proxy middleware isn't working on this system. The proxy config should route `/api/*` requests from the frontend to the backend, but the connection fails silently.

---

## Architecture

```
┌──────────────────┐      proxy       ┌──────────────────┐
│   Frontend        │  ──────────►   │   Backend        │
│   Vite :5173     │   (broken)      │   FastAPI :8090   │
└──────────────────┘                └──────────────────┘
        │                                   │
        ▼ (works)                          ▼ (works)
   Browser                           Direct curl
   Network tab                         test
```

---

## Solutions

### Solution A: Use Mock Data (Works Now)
```bash
# Add to .env file
VITE_USE_MOCK_API=true

# Restart
npm run dev
```

Pros: Works immediately  
Cons: Shows fake data, not real

---

### Solution B: Different Port (Try This First)
```bash
# Kill existing processes on 5173
pkill -f vite

# Run on different port
npx vite --port 5180

# Open http://127.0.0.1:5180/
```

Sometimes port 5173 has binding issues. Port 5180 often works.

---

### Solution C: Direct Backend Calls (Preferred)
Backend already has CORS enabled. Fix frontend to call directly:

```typescript
// src/lib/api/client.ts - already configured:
export const API_BASE_URL = 'http://127.0.0.1:8090';
export const USE_MOCK_API = false; // Set to false
```

This should work if browser CORS is not blocked.

---

### Solution D: Add CORS to Backend (Production Standard)
Already implemented in `ui/execution_api/app.py`:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Already enabled
    allow_headers=["*"],
)
```

CORS is enabled. The issue is the proxy in Vite.

---

### Solution E: Use Production Build (No Proxy Needed)
```bash
npm run build
npx vite preview --port 4173
```

Production build serves static files. Proxy still won't work in preview mode.

---

## Recommended Next Steps

1. **Try Solution B first** - Different port often fixes the issue
2. **If that fails, use Solution A** - Mock mode for development
3. **For production** - Deploy both services behind nginx with proper CORS headers

---

## Files Modified

- `src/lib/api/client.ts` - API client logic
- `vite.config.ts` - Proxy configuration
- `src/main.tsx` - React Query provider (production-ready)
- `src/components/common/PageErrorBoundary.tsx` - Error boundary
- `src/components/common/LoadingSkeleton.tsx` - Loading states
- `src/pages/RankingPage.tsx` - Now uses React Query
- `src/pages/PipelinePage.tsx` - Now uses React Query

---

## Testing Commands

```bash
# Check backend is running
lsof -i :8090 | grep LISTEN

# Test backend directly
curl -H "x-api-key: local-dev-key" localhost:8090/api/execution/ranking

# Check frontend port
lsof -i :5173 | grep LISTEN
```