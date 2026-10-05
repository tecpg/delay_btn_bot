-- Run once in the Supabase SQL editor before deploying the FCM backend change.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS fcm_token TEXT,
    ADD COLUMN IF NOT EXISTS platform TEXT,
    ADD COLUMN IF NOT EXISTS fcm_updated_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_users_fcm_token
    ON users (fcm_token)
    WHERE fcm_token IS NOT NULL;
