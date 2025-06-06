use std::env::args;
use util::{dump_out, for_each_tables, rename, NAMES};


fn main() {
    let args:Vec<_> = args().collect();
    let passwd_readonly = args[1].as_str();
    let passwd_optimizer = args[2].as_str();
    let years = args[3].as_str();
    let months = args[4].as_str();
    let years:Vec<_> = years.split_whitespace().collect();
    let months:Vec<_> = months.split_whitespace().collect();
    let dump_out = {
        |table: &str, ext: &str| {
        dump_out(passwd_readonly, table, ext);
    }};
    let rename = {
        |table: &str, ext: &str| {
        rename(passwd_optimizer, table, ext);
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &dump_out,
        &rename,
    ];
    for_each_tables(&years, &months, &handlers);
}

