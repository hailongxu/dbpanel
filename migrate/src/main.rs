use std::env::args;
use util::{self, for_each_name, for_each_tables, DatabaseEnv};

fn main() {
    let mut args = args();
    let Some(cmd) = args.next() else {
        eprintln!("Usage: migrate <config.toml>");
        std::process::exit(1);
    };
    let cfg = args.next();

    let env = util::load_env(cfg);
    let env_ro = util::to_ro_dbenv(&env);
    let env_rw = util::to_rw_dbenv(&env);
    let years:Vec<_> = env.years.split_whitespace().collect();
    let months:Vec<_> = env.months.split_whitespace().collect();


    match cmd.as_str() {
        "dump" => {
            dump(&env_ro,&years, &months, &env.postfix, &env.basedir);
        }
        "copy-test" => {
            copy_test(&env_rw);
        }
        "zip" => {
            zip(&env.basedir, &years);
        }
        "remove-postfix" => {
            remove_postfix(&env_rw, &years, &months, &env.postfix);
        }
        "take-to_postfix" => {
            take_to_postfix(&env_rw, &years, &months, &env.postfix);
        }
        "count" => {
            count(&env_ro, &years, &months, &env.postfix);
        }
        _ => {
            eprintln!("Unknown command: {cmd}");
            eprintln!("Available commands: dump");
            std::process::exit(2);
        }
    }
}

fn dump(env:&DatabaseEnv, years:&[&str], months:&[&str], postfix:&str, basedir:&str) {
    let dump_out = {
        |table: &str, _ext: &str| {
        let table = format!("{table}_{}", postfix);
        env.dump_out(&table,basedir);
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &dump_out,
    ];
    for_each_tables(&years, &months, &handlers);
}

fn copy_test(env_rw:&DatabaseEnv) {
    let table = "tablename";
    let table_new = &format!("{table}_new");
    println!("----------{table}, {table_new}");
    util::copy(&env_rw, table, table_new);
}

fn zip(basedir:&str, years:&[&str]) {
    for_each_name(&basedir,&years,util::zip);
}

fn remove_postfix(env_rw:&DatabaseEnv, years:&[&str], months:&[&str], postfix:&str) {
    let rename = {
        |table: &str, _ext: &str| {
        let table = format!("{table}_{}", postfix);
        util::remove_postfix(&env_rw, &table,postfix);
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &rename,
    ];
    // why does work when we use static [] ?????
    // let handlers = [
    //     &rename,
    // ];
    for_each_tables(&years, &months, &handlers);
}

fn take_to_postfix(env_rw:&DatabaseEnv, years:&[&str], months:&[&str], postfix:&str) {
    // let env = util::load_dump_env();
    // println!("Dumping tables for database: {:?}", env);
    // let env_rw = util::to_rw_dbenv(&env);
    // let years:Vec<_> = env.years.split_whitespace().collect();
    // let months:Vec<_> = env.months.split_whitespace().collect();
    let rename = {
        |table: &str, _ext: &str| {
        util::add_postfix(&env_rw,table, postfix);
    }};
    let create = {
        |table: &str, _ext: &str| {
        let src_table = &format!("{table}_{}", postfix);
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
    util::for_each_tables(&years, &months, &handlers);
}

fn count(env_ro:&DatabaseEnv, years:&[&str], months:&[&str], postfix:&str) {
    // let env = util::load_dump_env();
    // println!("Remove postfix from tables for database: {:?}", env);
    // let env_rw = util::to_rw_dbenv(&env);
    // let years:Vec<_> = env.years.split_whitespace().collect();
    // let months:Vec<_> = env.months.split_whitespace().collect();
    let count = {
        |table: &str, _ext: &str| {
        let table = format!("{table}_{}", postfix);
        util::count(&env_ro, &table);
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &count,
    ];
    // why does work when we use static [] ?????
    // let handlers = [
    //     &rename,
    // ];
    for_each_tables(&years, &months, &handlers);
}
