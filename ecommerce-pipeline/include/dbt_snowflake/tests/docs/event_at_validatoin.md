{% docs event_at_is_valid %}

This test is for checking the validity of the event timestamp.
event_at is timestamp_ntz type.
It has to satisfy the following conditions:
1. event_at is not null
2. event_at is in the past
3. event_at is not older than 2025-01-01

{% enddocs %}