use std::env::args;

fn main() {
    let env = load_env();
    let table = "tablename";
    let table_new = &format!("{table}_new");
    println!("----------{table}, {table_new}");
    copy(&env, table, table_new);
}
