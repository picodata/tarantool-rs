box.cfg {}

-- Idempotent on purpose: the container restarts during the test and
-- replays its WAL, so the space may already exist.
box.schema.space.create('reconnect', {if_not_exists = true})
box.space.reconnect:create_index('pk', {if_not_exists = true})
