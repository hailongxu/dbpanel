use util::{for_each_tables, remove_postfix};


fn main() {
    let env = util::load_dump_env();
    println!("Remove postfix from tables for database: {:?}", env);
    let env_rw = util::to_rw_dbenv(&env);
    let years:Vec<_> = env.years.split_whitespace().collect();
    let months:Vec<_> = env.months.split_whitespace().collect();
    let rename = {
        |table: &str, _ext: &str| {
        let table = format!("{table}_{}", env.postfix);
        remove_postfix(&env_rw, &table,&env.postfix);
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
