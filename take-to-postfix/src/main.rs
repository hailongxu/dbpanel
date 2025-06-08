use util::{for_each_tables, add_postfix,create_empty};


fn main() {
    let env = util::load_dump_env();
    println!("Dumping tables for database: {:?}", env);
    let env_rw = util::to_rw_dbenv(&env);
    let years:Vec<_> = env.years.split_whitespace().collect();
    let months:Vec<_> = env.months.split_whitespace().collect();
    let rename = {
        |table: &str, _ext: &str| {
        add_postfix(&env_rw,table, &env.postfix);
    }};
    let create = {
        |table: &str, _ext: &str| {
        let src_table = &format!("{table}_{}", env.postfix);
        let empty_table = &table;
        create_empty(&env_rw, src_table, empty_table);
    }};
    let handlers: Vec<&dyn Fn(&str, &str)> = vec![
        &rename,
        &create,
    ];
    // why does work when we use static [] ?????
    // let handlers = [
    //     &rename,
    // ];
    for_each_tables(&years, &months, &handlers);
}
