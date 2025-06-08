use std::env::args;
use util::{self, for_each_name, for_each_tables, DatabaseEnv};

fn main() {
    let mut args = args();
    args.next(); // ignore the self name
    let Some(cmd) = args.next() else {
        help();
        std::process::exit(1);
    };
    let cfg = args.next();
    let postfix = args.next().unwrap_or_default();

    let env = util::load_env(cfg);
    let env_ro = util::to_ro_dbenv(&env);
    let env_rw = util::to_rw_dbenv(&env);
    let years:Vec<_> = env.years.split_whitespace().collect();
    let months:Vec<_> = env.months.split_whitespace().collect();


    match cmd.as_str() {
        "dump" => {
            dump(&env_ro,&years, &months, &postfix, &env.basedir);
        }
        "copy" => {
            copy(&env_rw, &years, &months, &postfix);
        }
        "zip" => {
            zip(&env.basedir, &years);
        }
        "remove-postfix" => {
            remove_postfix(&env_rw, &years, &months, &postfix);
        }
        "take-to-postfix" => {
            take_to_postfix(&env_rw, &years, &months, &postfix);
        }
        "count" => {
            count(&env_ro, &years, &months, &postfix);
        }
        "drop" => {
            drop_table(&env_rw, &postfix);
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
        let table = combine(table, postfix);
        env.dump_out(&table,basedir);
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &dump_out,
    ];
    for_each_tables(&years, &months, &handlers);
}

fn copy(env_rw:&DatabaseEnv, years:&[&str], months:&[&str], postfix:&str) {
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
    for_each_tables(&years, &months, &handlers);
}

fn zip(basedir:&str, years:&[&str]) {
    for_each_name(&basedir,&years,util::zip);
}

fn remove_postfix(env_rw:&DatabaseEnv, years:&[&str], months:&[&str], postfix:&str) {
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
    for_each_tables(&years, &months, &handlers);
}

fn take_to_postfix(env_rw:&DatabaseEnv, years:&[&str], months:&[&str], postfix:&str) {
    // let env = util::load_dump_env();
    // println!("Dumping tables for database: {:?}", env);
    // let env_rw = util::to_rw_dbenv(&env);
    // let years:Vec<_> = env.years.split_whitespace().collect();
    // let months:Vec<_> = env.months.split_whitespace().collect();
    let postfix = if postfix.is_empty() {"old"} else {postfix};
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
        let table = combine(table, postfix);
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

fn drop_table(env_rw:&DatabaseEnv, table:&str) {
    util::drop(&env_rw,table);
}

fn combine(table:&str, postfix:&str)->String {
    let join = if postfix.is_empty() {""} else {"_"};
    let mut ret = String::with_capacity(table.len()+join.len()+postfix.len());
    ret.push_str(table);
    ret.push_str(join);
    ret.push_str(postfix);
    ret
}

fn help() {
    eprintln!(r"migrate copy|take-to-posfix|dump|zip|remove-postfix|count cfg <postfix>");
}
