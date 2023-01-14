# openget
A Spider FrameWork.

## Installation
You can install openget by simply doing:

    pip install openget
    
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