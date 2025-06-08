use std::{
    fmt::Debug, io::{BufRead as _, BufReader}, process::{Command, Stdio}
};
use std::process::ExitStatus;


pub const NAMES:[&str; 1] = [
    "tablename",
];

mod cfg;
// pub fn load_dump_env()->DumpEnv {
//     load_env::<DumpEnv>()
// }

// pub fn load_database_env()->DatabaseEnv {
//     load_env::<DatabaseEnv>()
// }

// pub fn load_zip_env()->ZipEnv {
//     load_env::<ZipEnv>()
// }

pub fn load_env<T>(cfg:Option<String>) -> T
where T: serde::de::DeserializeOwned+ Debug,
{
    // let cfg = std::env::args().nth(2);
    let cfg = cfg::get_cfg(cfg);
    println!("Loading configuration from: {:?}", cfg);
    let content = std::fs::read_to_string(&cfg)
        .expect("Failed to read configuration file");
    let cfg = toml::from_str::<T>(&content)
        .expect("Failed to parse configuration file");
    println!("Loaded configuration: {:?}", cfg);
    cfg
}

pub fn for_each_tables(years: &[&str], months: &[&str], handles: &[&dyn Fn(&str, &str)]) {
    for name in NAMES {
        for year in years {
            for month in months {
                for handle in handles {
                    let table = format!("{}{}{}", name, year, month);
                    handle(&table,"");
                }
            }
        }
    }
}

pub fn copy(dbw:&DatabaseEnv, table:&str, table_new:&str)->ExitStatus {
    //let create_struct_sql = format!("CREATE TABLE IF NOT EXISTS {table_new} like {table}");
    let create_struct_sql = format!("CREATE TABLE {table_new} like {table}");
    let status = dbw.exe_sql(&create_struct_sql);
    if !status.success() {
        return status;
    }
    let insert_data_sql = format!("insert into {table_new} SELECT * FROM {table}");
    dbw.exe_sql(&insert_data_sql)
}

pub fn create_empty(dbe:&DatabaseEnv, src_table:&str, empty_table:&str)->ExitStatus {
    let sql = format!("create table {empty_table} like {src_table}");
    dbe.exe_sql(&sql)
}

pub fn remove_postfix(dbe:&DatabaseEnv, table:&str, postfix:&str)->ExitStatus {
    assert!(table.ends_with(postfix));
    let src = table;
    let dst = table.strip_suffix(postfix).unwrap();
    rename(dbe, &[(&src, &dst)])
}

pub fn add_postfix(dbe:&DatabaseEnv, table:&str, postfix:&str)->ExitStatus {
    let src = table;
    let dst = format!("{table}_{postfix}");
    rename(dbe, &[(&src, &dst)])
}

pub fn count(dbe:&DatabaseEnv, table:&str)->ExitStatus {
    let sql = format!("select count(*) from {table}");
    dbe.exe_sql(&sql)
}

// pub fn restore_old(dbe:&DatabaseEnv, table:&str)->ExitStatus {
//     let old = format!("{table}_old");
//     rename(dbe, &[(&old,table)])
// }

pub fn rename(dbe:&DatabaseEnv, src_dst:&[(&str,&str)])->ExitStatus {
    let sql = src_dst.iter()
        .map(|(src, dst)| format!("alter table {src} rename to {dst}"))
        .collect::<Vec<_>>()
        .join("; ");
    dbe.exe_sql(&sql)
}


#[derive(Debug, serde::Deserialize)]
pub struct DumpEnv {
    url: String,
    user_ro: String,
    user_rw: String,
    passwd_ro: String,
    passwd_rw: String,
    database: String,
    pub years: String,
    pub months: String,
    pub postfix: String,
    pub basedir: String,
}

pub fn to_ro_dbenv(env:&DumpEnv)->DatabaseEnv {
    DatabaseEnv {
        url: env.url.clone(),
        user: env.user_ro.clone(),
        passwd: env.passwd_ro.clone(),
        database: env.database.clone(),
    }
}

pub fn to_rw_dbenv(env:&DumpEnv)->DatabaseEnv {
    DatabaseEnv {
        url: env.url.clone(),
        user: env.user_rw.clone(),
        passwd: env.passwd_rw.clone(),
        database: env.database.clone(),
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct ZipEnv {
    pub basedir: String,
    pub years: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct DatabaseEnv {
    url: String,
    user: String,
    passwd: String,
    database: String,
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

    pub fn dump_out(&self, table:&str, basedir:&str)->ExitStatus {
        let url = &self.url;
        let urlp = format!("-h{url}");
        let user = &self.user;
        let userp = format!("-u{}",user);
        let passwd = &self.passwd;
        let passwdp = format!("-p{}",passwd);
        let database = &self.database;
        let table_out = format!("{basedir}/{table}.sql");
        let mysqldump_cmd = format!("mysqldump {urlp} {userp} {passwdp} {database} {table} > {table_out}");
        println!("----- {mysqldump_cmd} ------");
        Command::new("sh")
            .arg("-c")
            .arg(mysqldump_cmd)
            .status()
            .expect("failed to execute process")
    }

    pub fn exe_sql(&self, sql:&str)->ExitStatus {
        let url = &self.url;
        let urlp = format!("-h{url}");
        let user = &self.user;
        let userp = format!("-u{}",user);
        let passwd = &self.passwd;
        let passwdp = format!("-p{}",passwd);
        let database = &self.database;
        let databasep  = format!("-D{}",database);
        let sqlp = format!("-e\"{sql}\"");
        println!("----- {sql} -----");
        let status = Command::new("mysql")
            .arg(urlp)
            .arg(userp)
            .arg(passwdp)
            .arg(databasep)
            .arg(sqlp)
            //.stdout(Stdio::piped())
            .status()
            .expect("failed to execute process");
    
        println!("process finished with: {status}");
        status
    }
}

pub fn for_each_name(basedir:&str,years: &[&str], handle: impl Fn(&str, &str, &str) -> ExitStatus) {
    for year in years {
        for name in NAMES {
            assert!(handle(basedir,year, name).success());
        }
    }
}

pub fn zip(basedir:&str,year:&str,name:&str)->ExitStatus {
    let dumpdir = format!("{basedir}/{year}");
    let zipfile = format!("{dumpdir}/{name}.zip");
    let zipsrc = format!("{dumpdir}/{name}*.sql");

    // for wildcard * is expanded by shell not by zip,
    // we should put zip in a sh shell
    // because Command's parent is not a shell, but a bin built by rustc 
    let zipcmd = format!("zip {zipfile} {zipsrc}");
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(zipcmd);
    println!("--------- {cmd:?} -----------");
    let status = cmd
        .status()
        .expect("failed to execute process");
    println!("process finished with: {status}");
    status
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = 4;
        assert_eq!(result, 4);
    }
}

