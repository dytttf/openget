# coding:utf8
import os
import argparse


def gen_env_example():
    from openget.env import env
    env_file_content = []
    for k, v in env.items():
        env_file_content.append("{}={}".format(
            k, v if v is not None and v != 0 else ""
        ))
    env_file_content = os.linesep.join(env_file_content)
    print(env_file_content)
    return


def get_cmd_args():
    parser = argparse.ArgumentParser(
        prog="Program Name (default: sys.argv[0])",
        description="openget 命令行解析",
        add_help=True,
    )
    parser.add_argument("--gen_env_example", action="store_true", help="生成环境变量示例文件")
    return parser.parse_args()


def main():
    # 命令行参数支持
    cmd_args = get_cmd_args()

    # 采集历史数据
    if cmd_args.gen_env_example:
        gen_env_example()
    return


if __name__ == '__main__':
    main()
