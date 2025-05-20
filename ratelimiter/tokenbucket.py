from time import monotonic
from abc import ABC, abstractmethod
import threading
from typing import Optional
import time

class RateLimiter(ABC):

    @abstractmethod
    def grant_access(self) -> bool:
        ...

class TokenBucketLimiter(RateLimiter):
    def __init__(self, capacity: int, refresh_rate: float):
        self.capacity = capacity
        self.refresh_rate = refresh_rate
        self.current_capacity = capacity
        self.last_filled = monotonic()
        self.lock = threading.Lock()

    def _refill_bucket(self):
        now = monotonic()
        elapsed = now - self.last_filled

        if elapsed > 0:
            to_add = round(elapsed * self.refresh_rate)
            self.current_capacity = min(self.capacity, self.current_capacity + to_add)
            self.last_filled = now

    def get_current_capacity(self) -> int:
        # read lock
        with self.lock:
            return self.current_capacity

    def grant_access(self) -> bool:
        with self.lock:
            self._refill_bucket()

            if self.current_capacity > 0:
                self.current_capacity -= 1
                return True
            return False

    def __repr__(self) -> str:
            return (f"TokenBucket(capacity={self.capacity}, "
                    f"tokens_per_second={self.refresh_rate}, "
                    f"current_tokens={self.current_capacity:.2f}, "
                    f"last_refill_ts={self.last_filled:.2f})")

class MultiKeyRateLimiter:
    def __init__(self, default_capacity: int, default_refresh_rate: float):
        self.buckets = {}
        self.default_refresh_rate = default_refresh_rate
        self.default_capacity = default_capacity
        self.lock = threading.Lock()

    def _get_or_create_bucket(self, key: str) -> TokenBucketLimiter:
        with self.lock:
            if key not in self.buckets:
                self.buckets[key] = TokenBucketLimiter(self.default_capacity, self.default_refresh_rate)

        return self.buckets[key]

    def allow_request(self, key: str) -> bool:
        bucket = self._get_or_create_bucket(key)

        return bucket.grant_access()

    def bucket_info(self, key: str) -> Optional[str]:
        with self.lock:
            if key in self.buckets:
                bucket = self.buckets[key]

                return f"""
                    Bucket - {key}
                    Current capacity - {bucket.get_current_capacity()}
                """
            return None

    def remove_bucket(self, key: str) -> bool:
        with self.lock:
            if key not in self.buckets:
                return False

            del self.buckets[key]
            return True

if __name__ == "__main__":
    print("--- Testing Single TokenBucket ---")
    bucket = TokenBucketLimiter(5, 1) # 5 tokens, 1 token/sec
    print(f"Initial bucket: {bucket}")

    print("\nAttempting to consume 3 tokens:")
    for i in range(3):
        print(f"Request {i+1}: Allowed = {bucket.grant_access()}")
    print(f"Bucket after 3 consumes: {bucket}") # Should have 2 tokens

    print("\nAttempting to consume 3 more tokens (should fail on the 3rd):")
    for i in range(3):
        print(f"Request {i+1}: Allowed = {bucket.grant_access()}")
    print(f"Bucket after 3 more attempts: {bucket}") # Should have 0 tokens

    print("\nWaiting for 2.5 seconds to refill...")
    time.sleep(2.5)
    # Manually check consumption to trigger refill and see state
    print(f"Attempting to consume 1 token: Allowed = {bucket.grant_access()}") # Should be allowed (approx 2.5 tokens refilled)
    print(f"Bucket state: {bucket}") # Should have ~1.5 tokens left

    print("\nWaiting for 10 seconds (bucket should fill to capacity):")
    time.sleep(10)
    print(f"Attempting to consume 1 token: Allowed = {bucket.grant_access()}")
    print(f"Bucket state: {bucket}") # Should have 4 tokens (capacity 5 - 1 consumed)
    print(f"Current tokens via getter: {bucket.get_current_capacity():.2f}") # should be approx 4

    # --- Test MultiKeyRateLimiter ---
    print("\n\n--- Testing MultiKeyRateLimiter ---")
    # Rate: 2 requests per second, burst of 5
    limiter = MultiKeyRateLimiter(5, 2)

    user1_key = "user:123"
    user2_key = "user:456"

    print(f"\nSimulating requests for {user1_key} (capacity 5, 2 tokens/sec):")
    for i in range(7):
        allowed = limiter.allow_request(user1_key)
        print(f"Request {i+1} for {user1_key}: Allowed = {allowed}. {limiter.bucket_info(user1_key)}")
        if i == 2: # After 3 requests
            print("Short pause (0.6s)...")
            time.sleep(0.6) # Should allow ~1.2 tokens to refill

    print(f"\nSimulating requests for {user2_key} (independent):")
    for i in range(3):
        allowed = limiter.allow_request(user2_key)
        print(f"Request {i+1} for {user2_key}: Allowed = {allowed}. {limiter.bucket_info(user2_key)}")

    print("\nWaiting for 3 seconds...")
    time.sleep(3)
    print(f"After 3s, checking {user1_key}:")
    allowed = limiter.allow_request(user1_key)
    print(f"Request for {user1_key}: Allowed = {allowed}. {limiter.bucket_info(user1_key)}")
    # user1 had 0.8 tokens (2 (cap) - 3 (req) + 1.2 (refill)) before the last two failed attempts.
    # So, current tokens for user1 were effectively 0 after the 7 requests loop.
    # After 3s sleep, 3s * 2 tokens/sec = 6 tokens should be added. Capped at 5.
    # So, allowing 1 request should leave 4 tokens.

    print(f"After 3s, checking {user2_key}:")
    allowed = limiter.allow_request(user2_key)
    print(f"Request for {user2_key}: Allowed = {allowed}. {limiter.bucket_info(user2_key)}")
    print("\n--- Thread Safety Test (Illustrative) ---")
    shared_key = "shared_resource"
    concurrent_limiter = MultiKeyRateLimiter(10, 5)
    def worker_task(worker_id: int, key: str):
        for i in range(7): # Each worker attempts 7 requests
            time.sleep(0.05 * worker_id) # Stagger starts slightly
            if concurrent_limiter.allow_request(key):
                print(f"Worker {worker_id}, Request {i+1} for {key}: ALLOWED")
            else:
                print(f"Worker {worker_id}, Request {i+1} for {key}: DENIED")
            time.sleep(0.1) # Small delay between requests by the same worker

    threads = []
    for i in range(3): # 3 workers
        thread = threading.Thread(target=worker_task, args=(i, shared_key))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
