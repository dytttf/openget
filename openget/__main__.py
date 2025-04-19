# coding:utf8
import json
import os
import argparse


def gen_env_example():
    from openget.env import env

    env_file_content = []
    for k, v in env.items():
        env_file_content.append("{}={}".format(k, v if v is not None and v != 0 else ""))
    env_file_content = os.linesep.join(env_file_content)
    print(env_file_content)
    return


def show_env_config():
    from openget.env import GLOBAL_ENV_FILE, global_env, LOCAL_ENV_FILE, local_env

    print(f"全局配置路径: {GLOBAL_ENV_FILE}")
    print(f"全局配置:\n{json.dumps(dict(global_env), indent=2, ensure_ascii=False)}")
    #
    print(f"\n当前配置路径: {LOCAL_ENV_FILE}")
    print(f"当前配置:\n{json.dumps(dict(local_env), indent=2, ensure_ascii=False)}")
    return


def get_cmd_args():
    parser = argparse.ArgumentParser(
        prog="Program Name (default: sys.argv[0])",
        description="openget 命令行解析",
        add_help=True,
    )
    parser.add_argument("--gen_env_example", action="store_true", help="生成环境变量示例文件")
    parser.add_argument("--show_env_config", action="store_true", help="查看配置文件")
    return parser.parse_args()


def main():
    # 命令行参数支持
    cmd_args = get_cmd_args()

    #
    if cmd_args.gen_env_example:
        gen_env_example()
    if cmd_args.show_env_config:
        show_env_config()
    return


if __name__ == "__main__":
    main()
