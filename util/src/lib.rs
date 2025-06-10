use std::
    process::Command
;
use std::io::Write;
use std::process::ExitStatus;
use std::process::Output;

mod cfg;
mod panelenv;
pub use panelenv::DatabaseEnv;
pub use panelenv::TableRule;
pub use panelenv::load_panel_env;

pub fn drop_without_confirm(dbw:&DatabaseEnv, table:&str)->ExitStatus {
    let sql = format!("DROP TABLE {table};");
    println!("You will \x1b[31mDROP\x1b[0m TABLE [\x1b[31m{table}\x1b[0m]!!!");
    exe_sql(dbw,&sql)
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
    exe_sql(dbw,&sql)
}

pub fn copy(dbw:&DatabaseEnv, table:&str, table_new:&str)->ExitStatus {
    let create_struct_sql = format!("CREATE TABLE {table_new} like {table}");
    let status = exe_sql(dbw,&create_struct_sql);
    if !status.success() {
        return status;
    }
    let insert_data_sql = format!("insert into {table_new} SELECT * FROM {table}");
    exe_sql(dbw,&insert_data_sql)
}

pub fn create_empty(dbe:&DatabaseEnv, src_table:&str, empty_table:&str)->ExitStatus {
    let sql = format!("create table {empty_table} like {src_table}");
    exe_sql(dbe,&sql)
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
    let output = exe_sql_with_output(dbe,&sql);
    assert!(output.status.success());
    let stdout = output.stdout.trim_ascii_end();
    println!("------------ {stdout:?} -----------");
    assert!(stdout.len()==1);
    stdout[0] == b'0'
}

pub fn count(dbe:&DatabaseEnv, table:&str)->(ExitStatus,String) {
    let sql = format!("select count(*) from {table}");
    let output = exe_sql_with_output(dbe,&sql);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let status = output.status;
    (status,stdout.to_string())
}

pub fn rename(dbe:&DatabaseEnv, src_dst:&[(&str,&str)])->ExitStatus {
    let sql = src_dst.iter()
        .map(|(src, dst)| format!("alter table {src} rename to {dst}"))
        .collect::<Vec<_>>()
        .join("; ");
    exe_sql(dbe,&sql)
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

pub fn dump_out(env_rw: &DatabaseEnv, table:&str, outdir:&str)->ExitStatus {
    let url = &env_rw.url;
    let urlp = format!("-h{url}");
    let user = &env_rw.user;
    let userp = format!("-u{}",user);
    let passwd = &env_rw.passwd;
    let passwdp = format!("-p{}",passwd);
    let database = &env_rw.database;
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

pub fn dump_in(env_rw: &DatabaseEnv, sqlfile:&str)->ExitStatus {
    let url = &env_rw.url;
    let urlp = format!("-h{url}");
    let user = &env_rw.user;
    let userp = format!("-u{}",user);
    let passwd = &env_rw.passwd;
    // let passwdp = format!("-p{}",passwd);
    let database = &env_rw.database;
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

pub fn exe_sql_with_output(env_rw: &DatabaseEnv, sql:&str)->Output {
    let url = &env_rw.url;
    let urlp = format!("-h{url}");
    let user = &env_rw.user;
    let userp = format!("-u{}",user);
    let passwd = &env_rw.passwd;
    // let passwdp = format!("-p{}",passwd);
    let database = &env_rw.database;
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
pub fn exe_sql(env_rw: &DatabaseEnv, sql:&str)->ExitStatus {
    let url = &env_rw.url;
    let urlp = format!("-h{url}");
    let user = &env_rw.user;
    let userp = format!("-u{}",user);
    let passwd = &env_rw.passwd;
    // let passwdp = format!("-p{}",passwd);
    let database = &env_rw.database;
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_env() {
        let result = 4;
        assert_eq!(result, 4);
    }
}

