# Chapter 4: Design a Rate Limiter

## Introduction
This chapter explores the design and implementation of a rate limiter—a system component used to control traffic rates sent by clients or services. Rate limiters are crucial for preventing abuse, reducing costs, and ensuring the stability of server resources. Examples of their use include limiting posts, account creations, and reward claims.

## Benefits of Rate Limiting
- **Preventing DoS Attacks:** Blocking excess calls to avoid resource starvation.
- **Cost Reduction:** Limiting unnecessary requests to reduce server expenses.
- **Preventing Overloads:** Filtering out excessive requests to stabilize server performance.

## Step 1: Understanding the Problem
### Key Features
- Server-side API rate limiter.
- Support for multiple throttle rules.
- Handle large-scale systems in distributed environments.
- Option for a standalone service or application-level code.
- Inform users when throttled.

### Requirements
- Accurate request throttling.
- Minimal latency.
- Low memory usage.
- Distributed capability.
- Clear exception handling.
- High fault tolerance.

## Step 2: High-Level Design
### Placement Options
<div style="margin-left:2rem">
    <img src="./images/rate_limiter_architecture.png"  alt="Rate Limiting Middleware Architecture" width="550">
</div>

1. **Client-Side Implementation:** Unreliable due to potential misuse.
2. **Server-Side Implementation:** Preferred for control and reliability.
3. **Middleware (API Gateway):** A flexible option for integrated rate limiting.


### Guidelines for Placement
- Evaluate current tech stack and choose efficient options.
- Select appropriate algorithms based on business needs.
- Use an API gateway if microservices are employed.
- Opt for commercial solutions if resources are limited.

## Step 3: Rate Limiting Algorithms
### 1. Token Bucket
<div style="margin-left:2rem">
  <img src="./images/token-bucket.png"  alt="Token Bucket Algorithm" width="550">
</div>

- **Description:** Tokens are added to a bucket at a fixed rate; each request consumes a token.
- **Parameters:** Bucket size and refill rate.
- **Pros:** Easy to implement, memory-efficient, supports traffic bursts.
- **Cons:** Requires careful parameter tuning.



### 2. Leaking Bucket
<div style="margin-left:2rem">
  <img src="./images/leaking-bucket.png"  alt="Leaking Bucket Algorithm" width="550">
</div>

- **Description:** Processes requests at a fixed rate using a FIFO queue.
- **Pros:** Memory-efficient, stable outflow rate.
- **Cons:** Traffic bursts may delay recent requests.
  

  Example: https://github.com/uber-go/ratelimit



### 3. Fixed Window Counter
<div style="margin-left:2rem">
  <img src="./images/fixed-window-counter.png"  alt="Fixed Window Counter" width="550">
</div>

- **Description:** Divides time into fixed intervals and uses counters to limit requests.
- **Pros:** Simple, efficient for specific use cases.
- **Cons:** Traffic spikes at window edges can exceed limits.

- Sudden burst of traffic at the edges of time windows
could cause more requests than allowed quota to go through.

  <img src="./images/fixed-window-issue.png"  alt="Fixed Window Issue" width="550">


### 4. Sliding Window Log
<div style="margin-left:2rem">
  <img src="./images/sliding-window-log.png"  alt="Sliding Window Log" width="550">
</div>

- **Description:** Tracks timestamps to allow a rolling time window.
- **Pros:** Accurate rate limiting.
- **Cons:** High memory consumption.
  


### 5. Sliding Window Counter
<div style="margin-left:2rem">
  <img src="./images/sliding-window-counter.png"  alt="Fixed Window Counter" width="550">
</div>

- **Description:** Combines fixed window and sliding log methods for smoothing spikes.
- **Pros:** Memory-efficient, handles traffic bursts.
- **Cons:** Approximation may not be perfectly strict.
  



## High-Level Architecture
<div style="margin-left:2rem">
  <img src="./images/architecture.png" style="margin-left: 40px; margin-top: 40px; margin-bottom: 20px;" alt="Architecture" width="550">
</div>

- **Data Storage:** Use in-memory caching (e.g., Redis) for fast counter operations.
- **Steps:**
  1. Client sends request to middleware.
  2. Middleware checks counters in Redis.
  3. Request is processed or rejected based on limits.


## Advanced Considerations
### Distributed Environments
- **Challenges:** Race conditions, synchronization issues.
- **Solutions:** Use locks, Lua scripts, or sorted sets in Redis. Employ centralized data stores for synchronization.

### Performance Optimizations
- Multi-data center setups for reduced latency.
- Eventual consistency models for synchronization.

### Monitoring
- Regular analytics to ensure algorithm effectiveness and adjust rules as needed.

## Impelementation 
### 1. Token Bucket (most common)
Tokens refill at a fixed rate; each request consumes one token.
```python
import time

class TokenBucket:
    def __init__(self, capacity, refill_rate):
        self.capacity = capacity          # max tokens
        self.tokens = capacity            # current tokens
        self.refill_rate = refill_rate    # tokens per second
        self.last_refill = time.time()

    def allow_request(self):
        self._refill()
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False  # rate limited

    def _refill(self):
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now
```

### 2. Sliding Window Counter (Redis-based, production-ready)
Tracks requests in a rolling time window — the most accurate approach.
```python
import redis
import time

class SlidingWindowRateLimiter:
    def __init__(self, redis_client, limit, window_seconds):
        self.redis = redis_client
        self.limit = limit
        self.window = window_seconds

    def is_allowed(self, client_id: str) -> bool:
        now = time.time()
        window_start = now - self.window
        key = f"rate_limit:{client_id}"

        pipe = self.redis.pipeline()
        pipe.zremrangebyscore(key, 0, window_start)   # remove old entries
        pipe.zadd(key, {str(now): now})               # add current request
        pipe.zcard(key)                               # count requests in window
        pipe.expire(key, self.window)
        results = pipe.execute()

        request_count = results[2]
        return request_count <= self.limit
```

### 3. Fixed Window Counter (simplest)
```python
class FixedWindowRateLimiter:
    def __init__(self, limit, window_seconds):
        self.limit = limit
        self.window = window_seconds
        self.counts = {}  # {client_id: (count, window_start)}

    def is_allowed(self, client_id: str) -> bool:
        now = time.time()
        count, window_start = self.counts.get(client_id, (0, now))

        if now - window_start >= self.window:   # new window
            count, window_start = 0, now

        if count < self.limit:
            self.counts[client_id] = (count + 1, window_start)
            return True
        return False
```

### 4. Middleware Integration (FastAPI example)
```python 
from fastapi import FastAPI, Request, HTTPException
from functools import wraps

app = FastAPI()
limiter = SlidingWindowRateLimiter(redis_client, limit=100, window_seconds=60)

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    client_ip = request.client.host

    if not limiter.is_allowed(client_ip):
        raise HTTPException(
            status_code=429,
            detail="Too Many Requests",
            headers={
                "Retry-After": "60",
                "X-RateLimit-Limit": "100",
                "X-RateLimit-Remaining": "0"
            }
        )

    response = await call_next(request)
    return response
```
Algorithm Comparison
AlgorithmAccuracyMemoryBurst HandlingBest ForToken BucketGoodLow✅ Allows burstsMost APIsSliding WindowBestMedium✅ SmoothProduction systemsFixed WindowPoor (boundary spikes)Low❌Simple use casesLeaky BucketGoodLow❌ Strict queueStreaming / smooth output

Key Design Decisions

Per IP vs per API key — API keys are more reliable (IPs can be shared via NAT)
Distributed systems — use Redis (atomic ops) instead of in-memory to share state across instances
Return proper headers — always send X-RateLimit-Limit, X-RateLimit-Remaining, Retry-After
Tiered limits — different limits per plan (free: 100/hr, pro: 10k/hr)

The Sliding Window + Redis approach is what most production APIs (GitHub, Stripe, OpenAI) use.