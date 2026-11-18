# 0002. 1-minute tumbling windows on event time with a 2-minute watermark

**Status:** Accepted

## Context
The dashboard shows activity and fraud per minute. Events can arrive late or out of order, for example when the producer is buffering or the network is slow. Grouping by arrival time would put them in the wrong minute.

## Decision
Group by `event_time`, the producer's timestamp, into 1-minute tumbling windows. A 2-minute watermark tells Spark how long to wait for late events before closing a window and discarding its state.

## Alternatives considered
- **Processing-time windows.** Simpler, but a delay in the pipeline would shift counts into later minutes.
- **Sliding windows (e.g. 5 minutes every minute).** Smoother charts, but each event is counted five times and the state is five times larger. Not needed for per-minute monitoring.
- **A longer watermark.** Accepts later events, but the dashboard waits longer for each minute.

## Consequences
- A window is written about 2 minutes after it ends, so the dashboard runs roughly 3 minutes behind real time.
- Events more than 2 minutes late are dropped from Gold. They are still kept in Bronze.
- Spark's state stays small: only the windows that are still open.
