use std::{io::{BufRead as _, BufReader}, process::{Command, Stdio}};



pub const NAMES:[&str; 1] = [
    "tablename",
];



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

fn get_exe_dir()->String {
    std::env::current_exe()
        .unwrap().parent()
        .unwrap().to_str()
        .unwrap().to_string()
}

pub fn dump_out(passwd:&str, table:&str,_:&str) {

    let passwd = format!("-p{}",passwd);
    let mut child = Command::new("mysqldump")
        .arg("-hxxx.xxx")
        .arg("-uuser")
        .arg(passwd)
        .arg("databasename")
        .arg(table)
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to execute process");
    // output.status;

    let stdout = child.stdout.take().expect("stdout should be available");
    let reader = BufReader::new(stdout);
    // reader.read_line()
    for line in reader.lines() {
        let line = line.unwrap();
        println!("{}", line);
    }
}

pub fn copy(passwd:&str, table:&str, table_new:&str) {
    let passwd = format!("-p{}",passwd);
    // let table = format!("{}{}{}",name,year,month);

    let copy_sql = format!("CREATE TABLE {table_new} AS SELECT * FROM {table}");
    let sql= copy_sql;
    
    let mut child = Command::new("mysql")
        .arg("-hxx.xx")
        .arg("-uuser")
        .arg(passwd)
        .arg("-Ddatabasename")
        .arg("-e")
        .arg(sql)
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to execute process");
    // output.status;

    // let aa = Command::get_program(&child);
    let stdout = child.stdout.take().expect("stdout should be available");
    let reader = BufReader::new(stdout);
    // reader.read_line()
    for line in reader.lines() {
        let line = line.unwrap();
        println!("{}", line);
    }
}



pub fn rename(passwd:&str, table:&str, _:&str) {
    let passwd = format!("-p{}",passwd);
    // let table = format!("{}{}{}",name,year,month);

    let sql_create_empty=format!("create table {table}_empty like {table}");
    let sql_rename_cur_to_old=format!("alter table {table} rename to {table}_old");
    let sql_rename_empty_to_cur=format!("alter table {table}_empty rename to {table}");
    let sql_drop=format!("drop table {table}_old");
    let sql=format!("\"{}; {}; {}; {};\"",sql_create_empty, sql_rename_cur_to_old, sql_rename_empty_to_cur, sql_drop);

    let mut child = Command::new("mysql")
        .arg("-hxxx.xxx")
        .arg("-uuserrw")
        .arg(passwd)
        .arg("-Ddatabasename")
        .arg("-e")
        .arg(sql)
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to execute process");
    // output.status;

    // let aa = Command::get_program(&child);
    let stdout = child.stdout.take().expect("stdout should be available");
    let reader = BufReader::new(stdout);
    // reader.read_line()
    for line in reader.lines() {
        let line = line.unwrap();
        println!("{}", line);
    }
}

pub fn for_each_name(basedir:&str,years: &[&str]) {
    for year in years {
        for name in NAMES {
            zip(basedir,year, name);
        }
    }
}

pub fn zip(basedir:&str,year:&str,table:&str) {
    let dumpdir=format!("{basedir}/{year}");
    let zipfile=format!("{dumpdir}/{table}.zip");
    let zipsrc = format!("{dumpdir}/{table}*.sql");
    // #zip $dumpdir/$table.zip "$dumpdir/?" 
    // zip $dumpdir/$zipfile.zip "$dumpdir/${zipfile}*.sql"
    let mut child = Command::new("zip")
        .arg(zipfile)
        .arg(zipsrc)
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to execute process");
    // output.status;

    let stdout = child.stdout.take().expect("stdout should be available");
    let reader = BufReader::new(stdout);
    for line in reader.lines() {
        let line = line.unwrap();
        println!("{}", line);
    }
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
