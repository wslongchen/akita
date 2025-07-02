/*
 *
 *  *
 *  *      Copyright (c) 2018-2025, SnackCloud All rights reserved.
 *  *
 *  *   Redistribution and use in source and binary forms, with or without
 *  *   modification, are permitted provided that the following conditions are met:
 *  *
 *  *   Redistributions of source code must retain the above copyright notice,
 *  *   this list of conditions and the following disclaimer.
 *  *   Redistributions in binary form must reproduce the above copyright
 *  *   notice, this list of conditions and the following disclaimer in the
 *  *   documentation and/or other materials provided with the distribution.
 *  *   Neither the name of the www.snackcloud.cn developer nor the names of its
 *  *   contributors may be used to endorse or promote products derived from
 *  *   this software without specific prior written permission.
 *  *   Author: SnackCloud
 *  *
 *
 */

use clap::Parser;
use akita_codegen::config::{AutoGenerator, prompt_for_config_path_or_default};

#[derive(Parser, Debug)]
#[command(author = "潘安", version = "0.5.0", about = "Akita Template Generator CLI Tool")]
struct Args {
    /// Path to the configuration file
    #[arg(short, long)]
    config: Option<String>,
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    print_copyright();
    // Parse command-line arguments
    let args = Args::parse();

    // 获取配置文件路径
    let config_path = match args.config {
        Some(path) => path, // 如果命令行指定路径，使用指定路径
        None => prompt_for_config_path_or_default("Please enter the configuration file path (press Enter to use default 'config.yaml')\n: ","config.yaml")?, // 提示用户输入路径或使用默认值
    };


    // Load or create configuration
    AutoGenerator::load_or_create_config(&config_path)
        .execute();
    println!("AutoGenerator excuted successfully\n");
    Ok(())
}

fn print_copyright() {
    eprintln!(
        r#"
  █████╗ ██╗  ██╗██╗████████╗ █████╗
 ██╔══██╗██║  ██║██║╚══██╔══╝██╔══██╗
 ███████║███████║██║   ██║   ███████║
 ██╔══██║██╔══██║██║   ██║   ██╔══██║
 ██║  ██║██║  ██║██║   ██║   ██║  ██║
 ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝   ╚═╝   ╚═╝  ╚═╝
 Akita - Database Template Engine

 Author  : 潘安
 Version : 0.5.0
 License : MIT
  "#
    );
}

