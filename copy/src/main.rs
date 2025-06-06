use std::env::args;

fn main() {
    let args:Vec<_> = args().collect();
    let passwd_optimizer = args[1].as_str();
    let table = args[2].as_str();
    let table_new = args[3].as_str();
    util::copy(passwd_optimizer, table, table_new);
}
