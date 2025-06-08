use util::{for_each_tables, rename};


fn main() {
    let env = util::load_dump_env();
    println!("Dumping tables for database: {:?}", env);
    let env_ro = util::to_ro_dbenv(&env);
    let env_rw = util::to_rw_dbenv(&env);
    let years:Vec<_> = env.years.split_whitespace().collect();
    let months:Vec<_> = env.months.split_whitespace().collect();
    let dump_out = {
        |table: &str, _ext: &str| {
        env_ro.dump_out(table,&env.basedir);
    }};
    let rename = {
        |table: &str, _ext: &str| {
        rename(&env_rw, table);
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &dump_out,
        //&rename,
    ];
    for_each_tables(&years, &months, &handlers);
}

