use crate::error::TitaniumError;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum WriteMod {
    // 立即同步到磁盘
    Sync,
    // 等待缓冲区满之后再自动刷新到磁盘
    Buffer,
}

pub const DEFAULT_CONFIG_FILE: &str = "titanium.conf";
pub const DEFAULT_DATA_DIR_PATH: &str = "./data";
pub const DEFAULT_MAX_KEY_SIZE: usize = 1024; // 1 KB
pub const DEFAULT_MAX_VALUE_SIZE: usize = 10 * 1024 * 1024; // 10 MB
pub const DEFAULT_WRITE_MOD: WriteMod = WriteMod::Sync;
pub const DEFAULT_MAX_FILE_SIZE: usize = 1073741824; // 1GB
static GLOBAL_WATCHER: OnceLock<ConfigWatcher> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: String,
    pub max_key_size: usize,
    pub max_val_size: usize,
    pub write_mod: WriteMod,
    pub max_file_size: usize,
}

impl Config {
    fn default() -> Self {
        Self {
            data_dir: DEFAULT_DATA_DIR_PATH.to_string(),
            max_key_size: DEFAULT_MAX_KEY_SIZE,
            max_val_size: DEFAULT_MAX_VALUE_SIZE,
            write_mod: DEFAULT_WRITE_MOD,
            max_file_size: DEFAULT_MAX_FILE_SIZE,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.max_key_size == 0 {
            return Err("max_key_size must be greater than 0".to_string());
        }
        if self.max_val_size == 0 {
            return Err("max_val_size must be greater than 0".to_string());
        }
        if self.max_file_size == 0 {
            return Err("max_file_size must be greater than 0".to_string());
        }
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self, TitaniumError> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let content = fs::read_to_string(path)?;
        let mut config = Self::default();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((key, value)) = line.split_once('=') {
                match key.trim() {
                    "data_dir" => config.data_dir = value.trim().to_string(),
                    "max_key_size" => {
                        config.max_key_size = value.trim().parse().map_err(|e| {
                            TitaniumError::ConfigError(format!(
                                "Invalid max_key_size '{}': {}",
                                value, e
                            ))
                        })?;
                    }
                    "max_val_size" => {
                        config.max_val_size = value.trim().parse().map_err(|e| {
                            TitaniumError::ConfigError(format!(
                                "Invalid max_val_size '{}': {}",
                                value, e
                            ))
                        })?;
                    }
                    "write_mod" => match value.trim().to_lowercase().as_str() {
                        "sync" => config.write_mod = WriteMod::Sync,
                        "buffer" => config.write_mod = WriteMod::Buffer,
                        unknown => {
                            return Err(TitaniumError::ConfigError(format!(
                                "Unknown write_mod variant: '{}'",
                                unknown
                            )));
                        }
                    },
                    _ => {}
                }
            }
        }

        if let Err(e) = config.validate() {
            return Err(TitaniumError::ConfigError(e));
        }

        Ok(config)
    }
}

#[derive(Clone)]
pub struct ConfigWatcher {
    inner: Arc<RwLock<Config>>,
    running: Arc<AtomicBool>,
}

impl ConfigWatcher {
    /// 初始化全局单例，通常在 main 函数开头调用一次
    pub fn init(path: impl Into<PathBuf>) -> Result<(), TitaniumError> {
        if GLOBAL_WATCHER.get().is_some() {
            return Ok(());
        }
        let watcher = Self::new(path)?;
        // 如果设置失败（说明并发初始化了），则停止当前创建的 watcher 线程
        if let Err(w) = GLOBAL_WATCHER.set(watcher) {
            w.stop();
        }
        Ok(())
    }

    /// 获取全局 ConfigWatcher 实例
    pub fn global() -> &'static Self {
        GLOBAL_WATCHER
            .get()
            .expect("ConfigWatcher not initialized. Call ConfigWatcher::init() first.")
    }

    /// 便捷方法：直接获取当前全局配置快照
    pub fn current() -> Config {
        Self::global().get()
    }

    pub fn new(path: impl Into<PathBuf>) -> Result<Self, TitaniumError> {
        let path = path.into();
        let config = Config::load(&path)?;
        let inner = Arc::new(RwLock::new(config));
        let running = Arc::new(AtomicBool::new(true));

        // 启动后台线程监控文件变化
        let watcher_inner = inner.clone();
        let watcher_running = running.clone();
        thread::spawn(move || {
            let mut last_mtime = fs::metadata(&path).and_then(|m| m.modified()).ok();
            while watcher_running.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(5)); // 每5秒检查一次
                if !watcher_running.load(Ordering::Relaxed) {
                    break;
                }

                // 1. 尝试获取最新的修改时间，如果获取失败（如文件被删除），直接跳过本次循环
                let mtime = match fs::metadata(&path).and_then(|m| m.modified()) {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                // 2. 如果修改时间没有变化，跳过
                if last_mtime == Some(mtime) {
                    continue;
                }

                println!("Config file changed, reloading...");
                match Config::load(&path) {
                    Ok(new_config) => {
                        *watcher_inner.write().expect("Config lock poisoned") = new_config;
                        last_mtime = Some(mtime);
                        println!("Config reloaded successfully.");
                    }
                    Err(e) => {
                        eprintln!("Failed to reload config: {}", e);
                    }
                }
            }
        });

        Ok(Self { inner, running })
    }

    /// 获取当前配置的快照
    pub fn get(&self) -> Config {
        self.inner.read().expect("Config lock poisoned").clone()
    }

    /// 轻量级获取 WriteMod，避免克隆整个 Config (包含 String 分配)
    pub fn write_mod(&self) -> WriteMod {
        self.inner
            .read()
            .expect("Config lock poisoned")
            .write_mod
            .clone()
    }

    /// 轻量级获取大小限制
    pub fn max_sizes(&self) -> (usize, usize) {
        let guard = self.inner.read().expect("Config lock poisoned");
        (guard.max_key_size, guard.max_val_size)
    }

    pub fn max_file_size(&self) -> usize {
        let guard = self.inner.read().expect("Config lock poisoned");
        guard.max_file_size
    }

    /// 停止后台监控线程
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// 允许在运行时（主要是测试中）覆盖全局配置
    /// 注意：这会影响所有使用 ConfigWatcher::global() 的组件
    pub fn override_config(&self, new_config: Config) {
        *self.inner.write().expect("Config lock poisoned") = new_config;
    }
}

impl Drop for ConfigWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}
