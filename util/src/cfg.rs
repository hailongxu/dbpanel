use std::{env::{args, current_dir, current_exe}, path::{Path, PathBuf}};

fn get_exe_dir()->String {
    std::env::current_exe()
        .unwrap().parent()
        .unwrap().to_str()
        .unwrap().to_string()
}

pub fn get_cfg(cfg:Option<String>)->PathBuf {
    if let Some(cfg) = cfg {
        let mut dir = current_dir().unwrap();
        // println!("Current directory: {:?}", dir);
        dir.push(cfg);
        dir
    } else {
        get_default_cfg()
    }
}

fn get_default_cfg()->PathBuf {
    // Step 1: Get the path of the current executable.
    let exe_path = current_exe()
        .expect("Failed to get current executable path");

    // Step 2: Extract the stem (file name without extension) from the executable path.
    let program_name = exe_path.file_stem()
        .expect("Failed to get file stem")
        .to_string_lossy();

    let config_file_name = format!("{}.toml", program_name);

    let config_dir = exe_path.parent().unwrap_or_else(|| Path::new(""));
    let config_path: PathBuf = config_dir.join(&config_file_name);

    config_path
}
