package balda

// Config holds the configuration for the Balda bot.
type Config struct {
	Relay RelayConfig `mapstructure:"balda"`
}

// RelayConfig holds the balda-specific configuration.
type RelayConfig struct {
	Provider          string                `mapstructure:"provider"`
	Telegram          TelegramConfig        `mapstructure:"telegram"`
	InboundWebhooks   InboundWebhooksConfig `mapstructure:"inbound_webhooks"`
	Logger            LoggerConfig          `mapstructure:"logger"`
	WorkingDir        string                `mapstructure:"working_dir"`
	StateDir          string                `mapstructure:"state_dir"`
	Sessions          SessionsConfig        `mapstructure:"sessions"`
	Memory            MemoryConfig          `mapstructure:"memory"`
	Goal              GoalConfig            `mapstructure:"goal"`
	Workspace         WorkspaceConfig       `mapstructure:"workspace"`
	MCPServers        []string              `mapstructure:"mcp_servers"`
	GlobalInstruction string                `mapstructure:"global_instruction"`
}

// TelegramConfig holds the Telegram bot configuration.
type TelegramConfig struct {
	Token          string        `mapstructure:"token"`
	FormattingMode string        `mapstructure:"formatting_mode"`
	PlanUpdates    bool          `mapstructure:"plan_updates"`
	Webhook        WebhookConfig `mapstructure:"webhook"`
}

// WebhookConfig holds Telegram webhook receiver settings.
type WebhookConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	ListenAddr string `mapstructure:"listen_addr"`
	Path       string `mapstructure:"path"`
	URL        string `mapstructure:"url"`
	AuthToken  string `mapstructure:"auth_token"`
}

// InboundWebhooksConfig controls generic authenticated inbound webhook ingestion.
type InboundWebhooksConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	ListenAddr string `mapstructure:"listen_addr"`
	Path       string `mapstructure:"path"`
	AuthToken  string `mapstructure:"auth_token"`
}

// LoggerConfig holds the logger configuration.
type LoggerConfig struct {
	Level  string `mapstructure:"level"`
	Pretty bool   `mapstructure:"pretty"`
}

// WorkspaceConfig controls balda Git workspace behavior.
type WorkspaceConfig struct {
	Mode       string `mapstructure:"mode"`
	BaseBranch string `mapstructure:"base_branch"`
}

type SessionsConfig struct {
	Persistence string `mapstructure:"persistence"`
}

type MemoryConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// GoalConfig controls /goal command execution behavior.
type GoalConfig struct {
	MaxIterations int `mapstructure:"max_iterations"`
}
