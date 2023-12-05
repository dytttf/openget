# openget
A Spider FrameWork.

## Installation
You can install openget by simply doing:

    pip install openget
    pip install git+https://github.com/dytttf/openget.git@main
    pip install git+https://github.com/dytttf/openget.git@dev

    
## Usage
**Very Important Thing**
This line must be the first line in code
```
from openget.spiders import *
```


## About Environment Variable
> ~/.openget/.env < $(pwd)/.env


## docker build
```shell
cd docekr
bash build.sh
```

## TODO
- ftp download
- auto add task
- statis crawl speed
- browser support: zhipin.com tmall.com
- sqlite.db rename
- use cmd to create spider template
- 金融许可证 卡死问题处理
- 环境变量定义优化
- 新项目创建优化