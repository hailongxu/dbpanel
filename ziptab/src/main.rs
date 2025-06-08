use util::*;

fn main() {
    let zip_env = load_zip_env();
    let years: Vec<_> = zip_env.years.split_whitespace().collect();
    let basedir = zip_env.basedir;
    for_each_name(&basedir,&years,zip);
}
