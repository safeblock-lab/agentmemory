# Foreground continuation for an existing reflection job

`mem::reflect-unit` normally processes durable reflection work under the configured recovery pause and its retry time. An operator can continue one existing ordinary job by passing its exact job ID and the persisted project and request ID:

```ts
await sdk.trigger({
  function_id: "mem::reflect-unit",
  payload: {
    jobId: "<existing-job-id>",
    foreground: {
      project: "<persisted-project>",
      requestId: "<persisted-request-id>",
    },
  },
});
```

Read those three values from the existing job first. The function checks that the job ID resolves to that job, that both persisted project fields match, and that the request ID matches. It rejects malformed controls, identity mismatches, and jobs created through the manual or selected-session flows. This call does not create a job or change its origin.

Each call executes at most the current reflection unit through the existing job lock, claim, source manifest, capture, and effect receipts. A `pending` response means another explicit call with the same three identifiers may continue the next unit. `preparing` means the job has persisted its preparation progress and may be called again. A terminal job is a no-op; an ambiguous provider outcome stays blocked and is not dispatched again. Existing retry, evidence, and effect idempotency checks still apply.

The foreground control applies only to that invocation. It does not enable recovery, drain the queue, or dispatch a follow-up durable unit. Calls without it retain the configured pause and retry-time behavior.
