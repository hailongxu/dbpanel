use std::env::args;
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
            count(&env_ro, &env.basedir, &years, &months, &postfix);
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
        |table: &str, year: &str| {
        let table = combine(table, postfix);
        let outdir = format!("{basedir}/{year}");
        env.dump_out(&table,&outdir);
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
    util::for_each_tables(&years, &months, &handlers);
}

fn count(env_ro:&DatabaseEnv, basedir:&str,years:&[&str], months:&[&str], postfix:&str) {
    let countpath = format!("{basedir}/{}-count.txt",env_ro.database);
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
    for_each_tables_mut(&years, &months, &mut handlers);
    writer.flush().unwrap();
}

fn drop_table(env_rw:&DatabaseEnv, table:&str) {
    util::drop(&env_rw,table);
}

fn combine(table:&str, postfix:&str)->String {
    let mut ret = String::with_capacity(table.len()+postfix.len());
    ret.push_str(table);
    ret.push_str(postfix);
    ret
}

fn help() {
    eprintln!(r"migrate copy|take-to-postfix|dump|zip|remove-postfix|count cfg <postfix>");
}
