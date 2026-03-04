# Job Runner

Shutdown behavior: when graceful shutdown starts, new jobs are rejected and all queued (not yet running) jobs are marked as `canceled`. Running jobs are allowed to finish until timeout.
