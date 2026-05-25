pub const CREATE_SCHEMA: &str = r#"
    CREATE TABLE IF NOT EXISTS messages (
        id BIGSERIAL PRIMARY KEY,
        mint TEXT NOT NULL,
        chat_id BIGINT NOT NULL,
        chat_name TEXT NOT NULL,
        text BYTEA NOT NULL,
        timestamp_us BIGINT NOT NULL,
        compressed BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_mint ON messages(mint);
    CREATE INDEX IF NOT EXISTS idx_timestamp ON messages(timestamp_us);
    CREATE INDEX IF NOT EXISTS idx_chat_id ON messages(chat_id);
    CREATE INDEX IF NOT EXISTS idx_mint_timestamp ON messages(mint, timestamp_us DESC);

    CREATE TABLE IF NOT EXISTS chats (
        chat_id BIGINT PRIMARY KEY,
        chat_name TEXT NOT NULL,
        last_seen_us BIGINT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_chat_name ON chats(chat_name);
    CREATE INDEX IF NOT EXISTS idx_chat_last_seen ON chats(last_seen_us DESC);
"#;

pub const INSERT_MESSAGE: &str = r#"
    INSERT INTO messages (mint, chat_id, chat_name, text, timestamp_us, compressed)
    VALUES ($1, $2, $3, $4, $5, $6)
"#;

pub const UPSERT_CHAT: &str = r#"
    INSERT INTO chats (chat_id, chat_name, last_seen_us)
    VALUES ($1, $2, $3)
    ON CONFLICT (chat_id)
    DO UPDATE SET
        chat_name = EXCLUDED.chat_name,
        last_seen_us = EXCLUDED.last_seen_us,
        updated_at = NOW()
"#;

pub const SELECT_MESSAGES_BY_MINT: &str = r#"
    SELECT chat_id, chat_name, text, timestamp_us, compressed
    FROM messages
    WHERE mint = $1
    ORDER BY timestamp_us DESC
"#;

pub const SELECT_CHAT_NAME_BY_ID: &str = r#"
    SELECT chat_name
    FROM chats
    WHERE chat_id = $1
"#;

pub const SELECT_CHAT_ID_BY_NAME: &str = r#"
    SELECT chat_id
    FROM chats
    WHERE chat_name = $1
"#;

pub const SELECT_UNIQUE_CHATS_FOR_MINT: &str = r#"
    SELECT DISTINCT chat_id, chat_name
    FROM messages
    WHERE mint = $1
    ORDER BY chat_name
"#;

pub const DELETE_OLD_MESSAGES: &str = r#"
    DELETE FROM messages
    WHERE timestamp_us < $1
"#;

pub const DELETE_ORPHAN_CHATS: &str = r#"
    DELETE FROM chats
    WHERE NOT EXISTS (
        SELECT 1
        FROM messages
        WHERE messages.chat_id = chats.chat_id
    )
"#;

pub const SELECT_STATS: &str = r#"
    SELECT
        COUNT(*) as total_messages,
        COUNT(DISTINCT mint) as unique_mints,
        COUNT(DISTINCT chat_id) as unique_chats,
        MIN(timestamp_us) as oldest_message,
        MAX(timestamp_us) as newest_message
    FROM messages
"#;

pub const SELECT_MINTS_FOR_CHAT_ID: &str = r#"
    SELECT DISTINCT mint
    FROM messages
    WHERE chat_id = $1
    ORDER BY mint
"#;

pub const SELECT_MINTS_FOR_CHAT_NAME: &str = r#"
    SELECT DISTINCT mint
    FROM messages
    WHERE chat_name = $1
    ORDER BY mint
"#;

pub const SELECT_CHATS_WITH_ANY_MINT: &str = r#"
    SELECT DISTINCT chat_id, chat_name
    FROM messages
    WHERE mint IS NOT NULL AND mint <> ''
    ORDER BY chat_name
"#;

pub const SELECT_CHANNELS_BY_SUBSTRING: &str = r#"
    SELECT chat_id, chat_name
    FROM chats
    WHERE chat_name ILIKE '%' || $1 || '%'
    ORDER BY chat_name
"#;

pub const SELECT_CONTEXTS_BY_CHANNEL_SUBSTRING: &str = r#"
    SELECT DISTINCT m.mint
    FROM messages m
    JOIN chats c ON c.chat_id = m.chat_id
    WHERE c.chat_name ILIKE '%' || $1 || '%'
    ORDER BY m.mint
"#;