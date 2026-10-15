# Skill provenance and reinforcement

Skill extraction identifies a canonical procedure by its normalized title, trigger, and steps. When another successful extraction identifies the same procedure, `sourceObservationIds` becomes the deduplicated union of the existing and current observation IDs. This also applies when new observations come from a session that already contributed to the procedure.

Reinforcement remains session based. A previously unseen session increases frequency once and adds 0.15 to strength, capped at 1. An existing session contributes new provenance without increasing frequency or strength.

Lifecycle effects retain the existing durable effect receipt and audit recovery. Replaying an applied lifecycle effect returns the stored procedure and repairs the audit without changing provenance, reinforcement, or timestamps and without repeating provider calls.

The extraction parser accepts 2 through 10 nonempty steps. An explicit `<no-skill/>` response remains a valid empty result. A response with fewer than 2 or more than 10 steps is malformed: its raw capture remains stored, no procedure is published, and replay uses the captured response without another provider call. The ordinary and fragmented extraction paths use the same parser.

This change does not rewrite previously acknowledged procedures or expand the selected extraction input limit. Selected inputs that require fragmentation continue to return `selected_skill_fragment_generation_unsupported`.
