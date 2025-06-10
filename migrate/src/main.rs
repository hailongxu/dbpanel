use std::env::args;
use util::TableRule;
use util::{self, for_each_name, for_each_tables, for_each_tables_mut, DatabaseEnv};
//use util::DEFAULT_POSTFIX;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;

fn main() {
    let mut args = args();
    args.next(); // ignore the self name
    let Some(cmd) = args.next() else {
        help();
        std::process::exit(1);
    };
    let cfg = args.next();
    let postfix = args.next().unwrap_or_default();

    let env = util::load_migrate_env(cfg);
    let env_ro = env.to_ro_dbenv();
    let env_rw = env.to_rw_dbenv();
    let rule = env.table_rule();

    let years:Vec<_> = env.years.split_whitespace().collect();
    let months:Vec<_> = env.months.split_whitespace().collect();


    match cmd.as_str() {
        "dumpout" => {
            dumpout(&env_ro,&rule, &postfix, &env.basedir);
        }
        "dumpin" => {
            dumpin(&env_ro,&rule, &postfix, &env.basedir);
        }
        "copy" => {
            copy(&env_rw, &rule, &postfix);
        }
        "zip" => {
            zip(&env.basedir, &rule);
        }
        "nameadd" => {
            add_postfix(&env_rw, &rule, &postfix);
        }
        "namendel" => {
            remove_postfix(&env_rw, &rule, &postfix);
        }
        "take" => {
            take_to_postfix(&env_rw, &rule, &postfix);
        }
        "count" => {
            count(&env_ro, &env.basedir, &rule, &postfix);
        }
        "empty" => {
            empty(&env_ro, &env.basedir, &rule, &postfix);
        }
        "drop" => {
            drop_table(&env_rw, &postfix);
        }
        "drop-empty" => {
            drop_empty_table(&env_rw, &rule, &postfix);
        }
        _ => {
            eprintln!("Unknown command: {cmd}");
            std::process::exit(2);
        }
    }
}

fn dumpout(env:&DatabaseEnv, rule:&TableRule, postfix:&str, basedir:&str) {
    let dump_out = {
        |table: &str, year: &str| {
        let table = combine(table, postfix);
        let outdir = format!("{basedir}/{year}");
        env.dump_out(&table,&outdir);
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &dump_out,
    ];
    for_each_tables(rule, &handlers);
}

fn dumpin(env:&DatabaseEnv, rule:&TableRule, postfix:&str, basedir:&str) {
    let dump_out = {
        |table: &str, year: &str| {
        let table = combine(table, postfix);
        let sqlfile = format!("{basedir}/{year}/{table}.sql");
        env.dump_in(&sqlfile);
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &dump_out,
    ];
    for_each_tables(rule, &handlers);
}

fn copy(env_rw:&DatabaseEnv, rule:&TableRule, postfix:&str) {
    let copy = {
        |table: &str, _ext: &str| {
        let table_new = combine(table, postfix);
        util::copy(&env_rw, &table, &table_new);
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &copy,
    ];
    // why does work when we use static [] ?????
    // let handlers = [
    //     &rename,
    // ];
    for_each_tables(rule, &handlers);
}

fn zip(basedir:&str, rule:&TableRule) {
    for_each_name(&basedir,rule,util::zip);
}

fn add_postfix(env_rw:&DatabaseEnv, rule:&TableRule, postfix:&str) {
    let rename = {
        |table: &str, _ext: &str| {
        util::add_postfix(&env_rw, &table, postfix);
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &rename,
    ];
    // why does work when we use static [] ?????
    // let handlers = [
    //     &rename,
    // ];
    for_each_tables(rule, &handlers);
}

fn remove_postfix(env_rw:&DatabaseEnv, rule:&TableRule, postfix:&str) {
    let rename = {
        |table: &str, _ext: &str| {
        let table = combine(table, postfix);
        util::remove_postfix(&env_rw, &table,postfix);
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &rename,
    ];
    // why does work when we use static [] ?????
    // let handlers = [
    //     &rename,
    // ];
    for_each_tables(rule, &handlers);
}

fn take_to_postfix(env_rw:&DatabaseEnv, rule:&TableRule, postfix:&str) {
    // let postfix = if postfix.is_empty() {DEFAULT_POSTFIX} else {postfix};
    let rename = {
        |table: &str, _ext: &str| {
        util::add_postfix(&env_rw,table, postfix);
    }};
    let create = {
        |table: &str, _ext: &str| {
        let src_table = &combine(table, postfix);
        let empty_table = &table;
        util::create_empty(&env_rw, src_table, empty_table);
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &rename,
        &create,
    ];
    // why does work when we use static [] ?????
    // let handlers = [
    //     &rename,
    // ];
    util::for_each_tables(rule, &handlers);
}

fn empty(env_ro:&DatabaseEnv, basedir:&str,rule:&TableRule, postfix:&str) {
    let countpath = format!("{basedir}/{}-empty{postfix}.txt",env_ro.database);
    eprintln!("----- empty file is at: {countpath} -----");
    let file = File::create(countpath).unwrap();
    let mut writer = BufWriter::new(file);
    let mut content = Vec::new();
    let mut handle  = {
        |table: &str, _ext: &str| {
        let table = combine(table, postfix);
        let is_empty = util::is_empty(&env_ro, &table);
        let out = if is_empty {b"1"} else {b"0"};
        println!("{table} : {is_empty}");
        let content_len = table.as_bytes().len() + 1 + out.len() + 1;
        content.clear();
        content.reserve(content_len);
        content.extend_from_slice(table.as_bytes());
        content.extend_from_slice(b" ");
        content.extend_from_slice(out);
        content.extend_from_slice(b"\n");
        writer.write_all(&content).unwrap();
    }};
    let mut handlers: Vec<&mut dyn FnMut(&str, &str)> = vec![
        &mut handle,
    ];
    // why does work when we use static [] ?????
    // let handlers = [
    //     &rename,
    // ];
    for_each_tables_mut(rule, &mut handlers);
    writer.flush().unwrap();
}

fn count(env_ro:&DatabaseEnv, basedir:&str,rule:&TableRule, postfix:&str) {
    let countpath = format!("{basedir}/{}-count{postfix}.txt",env_ro.database);
    eprintln!("----- count file is at: {countpath} -----");
    let file = File::create(countpath).unwrap();
    let mut writer = BufWriter::new(file);
    let mut content = Vec::new();
    let mut count = {
        |table: &str, _ext: &str| {
        let table = combine(table, postfix);
        let out = util::count(&env_ro, &table);
        let out = out.1.trim_end();
        println!("{table} : {out}");
        let content_len = table.as_bytes().len() + 1 + out.as_bytes().len() + 1;
        content.clear();
        content.reserve(content_len);
        content.extend_from_slice(table.as_bytes());
        content.extend_from_slice(b" ");
        content.extend_from_slice(out.as_bytes());
        content.extend_from_slice(b"\n");
        writer.write_all(&content).unwrap();
    }};
    let mut handlers: Vec<&mut dyn FnMut(&str, &str)> = vec![
        &mut count,
    ];
    // why does work when we use static [] ?????
    // let handlers = [
    //     &rename,
    // ];
    for_each_tables_mut(rule, &mut handlers);
    writer.flush().unwrap();
}

fn drop_table(env_rw:&DatabaseEnv, table:&str) {
    util::drop_with_confirm(&env_rw,table);
}

fn drop_empty_table(env_rw:&DatabaseEnv, rule:&TableRule, postfix:&str) {
    let handle = {
        |table: &str, _ext: &str| {
        let table = combine(table, postfix);
        if util::is_empty(&env_rw,&table) {
            println!("----- {table} is empty, and drop.");
            util::drop_without_confirm(&env_rw,&table);
        }
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &handle,
    ];
    // why does work when we use static [] ?????
    // let handlers = [
    //     &rename,
    // ];
    for_each_tables(rule, &handlers);
}

fn combine(table:&str, postfix:&str)->String {
    let mut ret = String::with_capacity(table.len()+postfix.len());
    ret.push_str(table);
    ret.push_str(postfix);
    ret
}

fn help() {
    eprintln!(r"migrate copy|take|dumpout|dumpin|zip|nameadd|namendel|count|empty|drop-empty cfg <postfix>");
}
