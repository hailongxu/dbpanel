use std::{
    fmt::Debug, io::{BufRead as _, BufReader}, process::{Command, Stdio}
};
use std::io::Write;
use std::process::ExitStatus;
use std::process::Output;

//pub const DEFAULT_POSTFIX:&str = "_old";
//pub const NAMES:[&str; 2] = [
//    "order_coupons",
//    "order_consignee",
//];
pub const NAMES:[&str; 11] = [
    "order_assoc_tenant",
    "order_consignee",
    "order_coupons",
    "order_info",
    "order_invoice",
    "order_items",
    "order_logistics",
    "order_memo",
    "order_print",
    "order_promotions",
    "order_spec_char",
];

mod cfg;
pub fn load_migrate_env(cfg:Option<String>)->MigateEnv {
    load_env::<MigateEnv>(cfg)
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

pub fn for_each_tables(years: &[&str], months: &[&str], handles: &[&dyn Fn(&str, &str)]) {
    for name in NAMES {
        for year in years {
            for month in months {
                for handle in handles {
                    let table = format!("{}{}{}", name, year, month);
                    handle(&table,year);
                }
            }
        }
    }
}

pub fn for_each_tables_mut(years: &[&str], months: &[&str], handles: &mut[&mut dyn FnMut(&str, &str)]) {
    for name in NAMES {
        for year in years {
            for month in months {
                for handle in &mut *handles {
                    let table = format!("{}{}{}", name, year, month);
                    handle(&table,year);
                }
            }
        }
    }
}

pub fn drop_without_confirm(dbw:&DatabaseEnv, table:&str)->ExitStatus {
    let sql = format!("DROP TABLE {table};");
    println!("You will \x1b[31mDROP\x1b[0m TABLE [\x1b[31m{table}\x1b[0m]!!!");
    dbw.exe_sql(&sql)
}

pub fn drop_with_confirm(dbw:&DatabaseEnv, table:&str)->ExitStatus {
    let sql = format!("DROP TABLE {table};");
    print!("You will \x1b[31mDROP\x1b[0m TABLE [\x1b[31m{table}\x1b[0m]?, input \x1b[31mDROP\x1b[0m to confirm: ");
    std::io::stdout().flush().unwrap();
    let mut input = String::new();
    std::io::stdin()
        .read_line(&mut input)
        .expect("read input failly");
    let input = input.trim_end();
    assert_eq!(input,"DROP");
    dbw.exe_sql(&sql)
}

pub fn copy(dbw:&DatabaseEnv, table:&str, table_new:&str)->ExitStatus {
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
    let dst = format!("{table}{postfix}");
    rename(dbe, &[(&src, &dst)])
}

pub fn is_empty(dbe:&DatabaseEnv, table:&str)->bool {
    let sql = format!("SELECT EXISTS(SELECT 1 FROM {table}) AS is_not_empty");
    let output = dbe.exe_sql_with_output(&sql);
    assert!(output.status.success());
    let stdout = output.stdout.trim_ascii_end();
    println!("------------ {stdout:?} -----------");
    assert!(stdout.len()==1);
    stdout[0] == b'0'
}

pub fn count(dbe:&DatabaseEnv, table:&str)->(ExitStatus,String) {
    let sql = format!("select count(*) from {table}");
    let output = dbe.exe_sql_with_output(&sql);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let status = output.status;
    (status,stdout.to_string())
}

pub fn rename(dbe:&DatabaseEnv, src_dst:&[(&str,&str)])->ExitStatus {
    let sql = src_dst.iter()
        .map(|(src, dst)| format!("alter table {src} rename to {dst}"))
        .collect::<Vec<_>>()
        .join("; ");
    dbe.exe_sql(&sql)
}


#[derive(Debug, serde::Deserialize)]
pub struct MigateEnv {
    url: String,
    user_ro: String,
    user_rw: String,
    passwd_ro: String,
    passwd_rw: String,
    database: String,
    pub years: String,
    pub months: String,
    pub basedir: String,
}

pub fn to_ro_dbenv(env:&MigateEnv)->DatabaseEnv {
    DatabaseEnv {
        url: env.url.clone(),
        user: env.user_ro.clone(),
        passwd: env.passwd_ro.clone(),
        database: env.database.clone(),
    }
}

pub fn to_rw_dbenv(env:&MigateEnv)->DatabaseEnv {
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

    pub fn dump_out(&self, table:&str, outdir:&str)->ExitStatus {
        let url = &self.url;
        let urlp = format!("-h{url}");
        let user = &self.user;
        let userp = format!("-u{}",user);
        let passwd = &self.passwd;
        let passwdp = format!("-p{}",passwd);
        let database = &self.database;
        let table_out = format!("{outdir}/{table}.sql");
        let mkoutdir = format!("mkdir -p {outdir}");
        let mysqldump_cmd = format!("{mkoutdir}; mysqldump {urlp} {userp} {passwdp} {database} {table} > {table_out}");
        println!("----- {database}/{table} => {outdir} ------");
        Command::new("sh")
            .arg("-c")
            .arg(mysqldump_cmd)
            .status()
            .expect("failed to execute process")
    }

    pub fn dump_in(&self, sqlfile:&str)->ExitStatus {
        let url = &self.url;
        let urlp = format!("-h{url}");
        let user = &self.user;
        let userp = format!("-u{}",user);
        let passwd = &self.passwd;
        // let passwdp = format!("-p{}",passwd);
        let database = &self.database;
        let databasep  = format!("-D{}",database);
        println!("----- {sqlfile} -----");
        let passwd_set = format!("export MYSQL_PWD={passwd}");
        // let passwdp = "";
        let mysql_cmd = format!("mysql {urlp} {userp} {databasep} < {sqlfile}");
        let passwd_unset = format!("unset MYSQL_PWD");
        let cmd = format!("{passwd_set};{mysql_cmd};{passwd_unset};");
        let status = Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .status()
            .expect("failed to execute process");
            
        println!("process finished with: {status}");
        status
    }

    pub fn exe_sql_with_output(&self, sql:&str)->Output {
        let url = &self.url;
        let urlp = format!("-h{url}");
        let user = &self.user;
        let userp = format!("-u{}",user);
        let passwd = &self.passwd;
        // let passwdp = format!("-p{}",passwd);
        let database = &self.database;
        let databasep  = format!("-D{}",database);
        let sqlp = format!("-e'{sql}'");
        println!("----- {sql} -----");
        let passwd_set = format!("export MYSQL_PWD={passwd}");
        let passwdp = "";
        let mysql_cmd = format!("mysql -NB {urlp} {userp} {databasep} {sqlp}");
        let passwd_unset = format!("unset MYSQL_PWD");
        let cmd = format!("{passwd_set};{mysql_cmd};{passwd_unset};");
        let output = Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .output()
            .expect("failed to execute process");
        
        println!("process finished with: {}",output.status);
        output
    }
    pub fn exe_sql(&self, sql:&str)->ExitStatus {
        let url = &self.url;
        let urlp = format!("-h{url}");
        let user = &self.user;
        let userp = format!("-u{}",user);
        let passwd = &self.passwd;
        // let passwdp = format!("-p{}",passwd);
        let database = &self.database;
        let databasep  = format!("-D{}",database);
        let sqlp = format!("-e'{sql}'");
        println!("----- {sql} -----");
        let passwd_set = format!("export MYSQL_PWD={passwd}");
        let passwdp = "";
        let mysql_cmd = format!("mysql {urlp} {userp} {databasep} {sqlp}");
        let passwd_unset = format!("unset MYSQL_PWD");
        let cmd = format!("{passwd_set};{mysql_cmd};{passwd_unset};");
        let status = Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .status()
            .expect("failed to execute process");
        
        // let mut status = Command::new("mysql");
        // status
        //     .arg(urlp)
        //     .arg(userp)
        //     .arg(passwdp)
        //     .arg(databasep)
        //     .arg(sqlp)
        //     //.env("MYSQL_PWD",passwd)
        //     ;
        //  //println!("------- {status:?} -------");
        //  let status = status
        //     //.stdout(Stdio::piped())
        //     .status()
        //     .expect("failed to execute process");
    
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
    let zipfile = format!("{dumpdir}/{name}{year}.zip");
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

