---
description: Developer identity, public documentation privacy, and console encoding invariants
always_on: true
---

# Developer Preferences & Environmental Safety

## 1. Identity & Public Attribution
- In all public documentation, README files, licenses, docstrings, and comments, **never publish full names**.
- Always use the GitHub username **`@pushkarreddyy`** for all attribution and credits.
- For Git commits when pushing to GitHub, always use the user's configured GitHub noreply address (`212133856+pushkarreddyy@users.noreply.github.com`) to prevent `GH007` private email push rejections.

## 2. Windows Console & Script Encoding
- On Windows environments, do NOT output 4-byte Unicode emojis in scripts or CLI tools intended for stdout.
- Standard code pages (`cp1252`) will raise `UnicodeEncodeError`. Use ASCII markers such as `[PASS]`, `[SUCCESS]`, `[ERROR]`, `[INFO]`.

## 3. Rate Limiter Numerical & Boundary Safety
- **Underflow Protection**: Always clamp `remaining_quota` to `max(0, limit - count)`. Never allow negative remaining quota.
- **Clock Skew / Negative Retry-After**: If client/server timestamps produce negative retry durations due to clock skew, clamp `retry_after = max(1.0, calculated_value)`.
- **Division-by-Zero Invariant**: Guard all metric averaging calculations against zero counts (`count > 0 ? sum / count : 0.0`).
- **Timestamp Collision Mitigation**: When using Redis Sorted Sets with epoch scores, append a unique request ID or nanosecond salt to member keys (`f"{timestamp}:{uuid}"`) to avoid member deduplication on simultaneous requests.
