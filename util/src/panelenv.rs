use std::{fmt::Debug, process::ExitStatus};
use super::cfg;

pub fn load_panel_env(cfg:Option<String>)->PanelEnv {
    load_env::<PanelEnv>(cfg)
}

pub fn load_env<T>(cfg:Option<String>) -> T
where T: serde::de::DeserializeOwned+ Debug,
{
    println!("-------- {cfg:?} --------");
    let cfg = cfg::get_cfg(cfg);
    println!("Loading configuration from: {:?}", cfg);
    let content = std::fs::read_to_string(&cfg)
        .expect("Failed to read configuration file");
    let cfg = toml::from_str::<T>(&content)
        .expect("Failed to parse configuration file");
    println!("Loaded configuration.");
    cfg
}

#[derive(Debug, serde::Deserialize)]
pub struct PanelEnv {
    url: String,
    user_ro: String,
    user_rw: String,
    passwd_ro: String,
    passwd_rw: String,
    database: String,
    pub names: Vec<String>,
    pub years: String,
    pub months: String,
    pub basedir: String,
}

impl PanelEnv {
    pub fn to_ro_dbenv(&self)->DatabaseEnv {
        DatabaseEnv {
            url: self.url.clone(),
            user: self.user_ro.clone(),
            passwd: self.passwd_ro.clone(),
            database: self.database.clone(),
        }
    }

    pub fn to_rw_dbenv(&self)->DatabaseEnv {
        DatabaseEnv {
            url: self.url.clone(),
            user: self.user_rw.clone(),
            passwd: self.passwd_rw.clone(),
            database: self.database.clone(),
        }
    }

    pub fn table_rule(&self)->TableRule {
        let names = self.names.iter().map(
            |e|e.as_str()).collect();
        let years = self.years.split_whitespace().collect();
        let months = self.months.split_whitespace().collect();
        TableRule {
            names,
            years,
            months,
        }
    }

}

#[derive(Debug, serde::Deserialize)]
pub struct ZipEnv {
    pub basedir: String,
    pub years: String,
}

pub struct TableRule<'a> {
    pub names: Vec<&'a str>,
    pub years: Vec<&'a str>,
    pub months: Vec<&'a str>,
}

impl TableRule<'_> {
    pub fn for_each_name(&self,basedir:&str, handle: impl Fn(&str, &str, &str) -> ExitStatus) {
        for year in &self.years {
            for name in &self.names {
                assert!(handle(basedir,year, name).success());
            }
        }
    }

    pub fn for_each_tables(&self, handles: &[&dyn Fn(&str, &str, usize)]) {
        let mut i = 0;
        for name in &self.names {
            for year in &self.years {
                for month in &self.months {
                    for handle in handles {
                        let table = format!("{}{}{}", name, year, month);
                        handle(&table,year, i);
                        i += 1;
                    }
                }
            }
        }
    }

    pub fn for_each_tables_mut(&self, handles: &mut[&mut dyn FnMut(&str, &str)]) {
        for name in &self.names {
            for year in &self.years {
                for month in &self.months {
                    for handle in &mut *handles {
                        let table = format!("{}{}{}", name, year, month);
                        handle(&table,year);
                    }
                }
            }
        }
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct DatabaseEnv {
    pub(crate) url: String,
    pub(crate) user: String,
    pub(crate) passwd: String,
    pub database: String,
}

impl DatabaseEnv {
    pub fn new()->Self {
        Self {
            url: String::new(),
            user: String::new(),
            passwd: String::new(),
            database: String::new(),
        }
    }

    pub fn from(url:&str,user:&str,passwd:&str,db:&str)->Self {
        Self {
            url: url.into(),
            user: user.into(),
            passwd: passwd.into(),
            database: db.into(),
        }
    }

    pub fn init(&mut self, url:&str,user:&str,passwd:&str,db:&str) {
        self.url.push_str(url);
        self.user.push_str(user);
        self.passwd.push_str(passwd);
        self.database.push_str(db);
    }
}
