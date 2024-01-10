use {
    serde::{de, Deserialize, Deserializer},
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    solana_sdk::pubkey::Pubkey,
    std::{collections::HashSet, fs::read_to_string, net::SocketAddr, path::Path},
    tokio::sync::Semaphore,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub libpath: String,
    #[serde(default)]
    pub log: ConfigLog,
    pub grpc: ConfigGrpc,
    #[serde(default)]
    pub prometheus: Option<ConfigPrometheus>,
    /// Action on block re-construction error
    #[serde(default)]
    pub block_fail_action: ConfigBlockFailAction,
}

impl Config {
    fn load_from_str(config: &str) -> PluginResult<Self> {
        serde_json::from_str(config).map_err(|error| GeyserPluginError::ConfigFileReadError {
            msg: error.to_string(),
        })
    }

    pub fn load_from_file<P: AsRef<Path>>(file: P) -> PluginResult<Self> {
        let config = read_to_string(file).map_err(GeyserPluginError::ConfigFileOpenError)?;
        Self::load_from_str(&config)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigLog {
    /// Log level.
    #[serde(default = "ConfigLog::default_level")]
    pub level: String,
}

impl Default for ConfigLog {
    fn default() -> Self {
        Self {
            level: Self::default_level(),
        }
    }
}

impl ConfigLog {
    fn default_level() -> String {
        "info".to_owned()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpc {
    /// Address of Grpc service.
    pub address: SocketAddr,
    /// TLS config
    pub tls_config: Option<ConfigGrpcServerTls>,
    /// Capacity of the channel used for accounts from snapshot,
    /// on reaching the limit Sender block validator startup.
    #[serde(
        default = "ConfigGrpc::snapshot_plugin_channel_capacity_default",
        deserialize_with = "deserialize_usize_str_maybe"
    )]
    pub snapshot_plugin_channel_capacity: Option<usize>,
    /// Capacity of the client channel, applicable only with snapshot
    #[serde(
        default = "ConfigGrpc::snapshot_client_channel_capacity_default",
        deserialize_with = "deserialize_usize_str"
    )]
    pub snapshot_client_channel_capacity: usize,
    /// Capacity of the channel per connection
    #[serde(
        default = "ConfigGrpc::channel_capacity_default",
        deserialize_with = "deserialize_usize_str"
    )]
    pub channel_capacity: usize,
    /// Concurrency limit for unary requests
    #[serde(
        default = "ConfigGrpc::unary_concurrency_limit_default",
        deserialize_with = "deserialize_usize_str"
    )]
    pub unary_concurrency_limit: usize,
    /// Enable/disable unary methods
    #[serde(default)]
    pub unary_disabled: bool,
    /// Limits for possible filters
    #[serde(default)]
    pub filters: ConfigGrpcFilters,

    /// Addresses excluded by carpool
    #[serde(default, deserialize_with = "deserialize_pubkey_set")]
    pub carpool_exclude: HashSet<Pubkey>,
}

impl ConfigGrpc {
    const fn snapshot_plugin_channel_capacity_default() -> Option<usize> {
        None
    }

    const fn snapshot_client_channel_capacity_default() -> usize {
        50_000_000
    }

    const fn channel_capacity_default() -> usize {
        250_000
    }

    const fn unary_concurrency_limit_default() -> usize {
        Semaphore::MAX_PERMITS
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcServerTls {
    pub cert_path: String,
    pub key_path: String,
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcFilters {
    pub accounts: ConfigGrpcFiltersAccounts,
    pub slots: ConfigGrpcFiltersSlots,
    pub transactions: ConfigGrpcFiltersTransactions,
    pub blocks: ConfigGrpcFiltersBlocks,
    pub blocks_meta: ConfigGrpcFiltersBlocksMeta,
    pub entry: ConfigGrpcFiltersEntry,
}

impl ConfigGrpcFilters {
    pub fn check_max(len: usize, max: usize) -> anyhow::Result<()> {
        anyhow::ensure!(
            len <= max,
            "Max amount of filters reached, only {} allowed",
            max
        );
        Ok(())
    }

    pub fn check_any(is_empty: bool, any: bool) -> anyhow::Result<()> {
        anyhow::ensure!(
            !is_empty || any,
            "Broadcast `any` is not allowed, at least one filter required"
        );
        Ok(())
    }

    pub fn check_pubkey_max(len: usize, max: usize) -> anyhow::Result<()> {
        anyhow::ensure!(
            len <= max,
            "Max amount of Pubkeys reached, only {} allowed",
            max
        );
        Ok(())
    }

    pub fn check_pubkey_reject(pubkey: &Pubkey, set: &HashSet<Pubkey>) -> anyhow::Result<()> {
        anyhow::ensure!(
            !set.contains(pubkey),
            "Pubkey {} in filters not allowed",
            pubkey
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcFiltersAccounts {
    pub max: usize,
    pub any: bool,
    pub account_max: usize,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub account_reject: HashSet<Pubkey>,
    pub owner_max: usize,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub owner_reject: HashSet<Pubkey>,
}

impl Default for ConfigGrpcFiltersAccounts {
    fn default() -> Self {
        Self {
            max: usize::MAX,
            any: true,
            account_max: usize::MAX,
            account_reject: HashSet::new(),
            owner_max: usize::MAX,
            owner_reject: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcFiltersSlots {
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max: usize,
}

impl Default for ConfigGrpcFiltersSlots {
    fn default() -> Self {
        Self { max: usize::MAX }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcFiltersTransactions {
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max: usize,
    pub any: bool,
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub account_include_max: usize,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub account_include_reject: HashSet<Pubkey>,
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub account_exclude_max: usize,
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub account_required_max: usize,
}

impl Default for ConfigGrpcFiltersTransactions {
    fn default() -> Self {
        Self {
            max: usize::MAX,
            any: true,
            account_include_max: usize::MAX,
            account_include_reject: HashSet::new(),
            account_exclude_max: usize::MAX,
            account_required_max: usize::MAX,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcFiltersBlocks {
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max: usize,
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub account_include_max: usize,
    #[serde(alias = "any")]
    pub account_include_any: bool,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub account_include_reject: HashSet<Pubkey>,
    pub include_transactions: bool,
    pub include_accounts: bool,
    pub include_entries: bool,
}

impl Default for ConfigGrpcFiltersBlocks {
    fn default() -> Self {
        Self {
            max: usize::MAX,
            account_include_max: usize::MAX,
            account_include_any: true,
            account_include_reject: HashSet::new(),
            include_transactions: true,
            include_accounts: true,
            include_entries: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcFiltersBlocksMeta {
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max: usize,
}

impl Default for ConfigGrpcFiltersBlocksMeta {
    fn default() -> Self {
        Self { max: usize::MAX }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcFiltersEntry {
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max: usize,
}

impl Default for ConfigGrpcFiltersEntry {
    fn default() -> Self {
        Self { max: usize::MAX }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigPrometheus {
    /// Address of Prometheus service.
    pub address: SocketAddr,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConfigBlockFailAction {
    Log,
    Panic,
}

impl Default for ConfigBlockFailAction {
    fn default() -> Self {
        Self::Log
    }
}

fn deserialize_usize_str<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Integer(usize),
        String(String),
    }

    match Value::deserialize(deserializer)? {
        Value::Integer(value) => Ok(value),
        Value::String(value) => value
            .replace('_', "")
            .parse::<usize>()
            .map_err(de::Error::custom),
    }
}

fn deserialize_usize_str_maybe<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Integer(usize),
        String(String),
    }

    match Option::<Value>::deserialize(deserializer)? {
        Some(Value::Integer(value)) => Ok(Some(value)),
        Some(Value::String(value)) => value
            .replace('_', "")
            .parse::<usize>()
            .map(Some)
            .map_err(de::Error::custom),
        None => Ok(None),
    }
}

fn deserialize_pubkey_set<'de, D>(deserializer: D) -> Result<HashSet<Pubkey>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<&str>::deserialize(deserializer)?
        .into_iter()
        .map(|value| {
            value
                .parse()
                .map_err(|error| de::Error::custom(format!("Invalid pubkey: {value} ({error:?})")))
        })
        .collect::<Result<_, _>>()
}
