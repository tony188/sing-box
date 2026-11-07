package urltest

import "time"

// Shared by scheduled URLTest checks, failure confirmations and load balancing.
// Explicit manual probe timeouts and application dialing keep their own budgets.
const BackgroundProbeTimeout = 5 * time.Second
