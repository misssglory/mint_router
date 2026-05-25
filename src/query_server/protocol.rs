pub const CMD_GET: &str = "GET";
pub const CMD_HELP: &str = "HELP";
pub const CMD_CLEANUP: &str = "CLEANUP";

pub const GET_MINT: &str = "mint";
pub const GET_CHAT_ID: &str = "chat:id";
pub const GET_CHAT_NAME: &str = "chat:name";
pub const GET_CHAT_ID_FOR_NAME: &str = "chat:id-for-name";
pub const GET_STATS: &str = "stats";
pub const GET_CHANNELS_WITH_MINTS: &str = "channels-with-mints";
pub const GET_CHANNEL_MINTS: &str = "channel-mints";
pub const GET_CONTEXTS_BY_CHANNEL: &str = "contexts-by-channel";

pub fn help_commands() -> Vec<&'static str> {
    vec![
        "GET mint <address> [--no-cache] - Get all messages for a mint address",
        "GET mint <address> --unique-chats - Get unique chats where mint was mentioned",
        "GET chat:id <id> - Get chat name by ID",
        "GET chat:name <name> - Get chat ID by name",
        "GET chat:id-for-name <name> - Get chat ID only",
        "GET channels-with-mints - Get all channels that have at least one stored context",
        "GET channel-mints chatid <id> - Get all contexts for a channel by ID",
        "GET channel-mints chatname <name> - Get all contexts for a channel by exact name",
        "GET contexts-by-channel <substring> - Get contexts where matched channel names contain substring (case-insensitive)",
        "GET stats - Get database statistics",
        "CLEANUP <max_age_seconds> - Delete messages older than specified seconds",
        "HELP - Show this help",
    ]
}