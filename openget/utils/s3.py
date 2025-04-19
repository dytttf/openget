# coding:utf8
import json
import os
import hashlib
import sys
import oss2
import tempfile
from typing import Generator
from oss2 import SizedFileAdapter, determine_part_size, Bucket
from oss2.models import PartInfo, SimplifiedObjectInfo
from openget.utils import log

temp_dir = tempfile.mkdtemp()


class OSSHandler(object):
    """https://help.aliyun.com/document_detail/32027.html"""

    def __init__(
        self,
        AccessKeyId="",
        AccessKeySecret="",
        endpoint="",
        bucket_name="",
        logger=None,
    ):
        self.AccessKeyId = AccessKeyId
        self.AccessKeySecret = AccessKeySecret
        self.bucket_name = bucket_name
        if not self.bucket_name:
            raise ValueError("bucket name must not empty")
        self.endpoint = endpoint
        if not endpoint:
            raise ValueError("endpoint must not empty")

        self.logger = logger or log.get_logger(__file__)

        self.bucket: Bucket = None
        self.init()

        # list obj 迭代过程中游标记录
        self.list_next_marker = ""

    def init(self):
        try:
            auth = oss2.Auth(self.AccessKeyId, self.AccessKeySecret)
            self.bucket = oss2.Bucket(auth, self.endpoint, self.bucket_name)
        except Exception as e:
            self.logger.exception(e)
        return

    def download(
        self,
        filename: str = "",
        local_file_name: str = "",
        local_dir: str = "",
        delimiter: str = "/",
        show_progress: bool = False,
        **kwargs,
    ):
        """
        文件下载
        Args:
            filename: oss文件全名 包含路径
            local_file_name: 下载到本地之后的文件名  若不指定则从filename获取 按路径切分到最后一级
            local_dir: 本地目录
            delimiter: 路径切割符
            show_progress: 是否显示下载进度
            **kwargs:

        Returns:

        """
        if not local_file_name:
            local_file_name = filename.split(delimiter)[-1]
        local_file_name = os.path.join(local_dir, local_file_name)

        def progress_callback(consumed_bytes, total_bytes):
            if total_bytes:
                rate = 100 * (float(consumed_bytes) / float(total_bytes))
                print("\r下载 {} -> {:.2f}%".format(local_file_name, rate), end="")
                sys.stdout.flush()

        res = self.bucket.get_object_to_file(
            filename,
            local_file_name,
            progress_callback=progress_callback if show_progress else None,
        )
        if show_progress:
            print("\r")
        self.logger.debug("oss文件下载成功: {} > {}".format(filename, local_file_name))
        return res

    def download_object(self, filename, show_progress: bool = False, **kwargs):
        """
            文件下载
        Args:
            filename: oss文件全名 包含路径
            show_progress: 是否显示下载进度
            **kwargs:

        Returns:
            file-like object

        """

        def progress_callback(consumed_bytes, total_bytes):
            if total_bytes:
                rate = 100 * (float(consumed_bytes) / float(total_bytes))
                print("\r下载 {} -> {:.2f}%".format(filename, rate), end="")
                sys.stdout.flush()

        res = self.bucket.get_object(filename, progress_callback=progress_callback if show_progress else None)
        return res

    def download_prefix(self, prefix: str = "", local_dir: str = "", delimiter: str = "/", **kwargs):
        """
            批量下载
        Args:
            prefix:
            local_dir:
            delimiter:
            **kwargs:

        Returns:

        """
        for obj in self.list(prefix=prefix):
            filename = obj.key
            if filename.endswith(delimiter):
                continue
            self.download(filename=filename, local_dir=local_dir, delimiter=delimiter)
        return

    def exists(self, file_name=None, remote_dir=None, root_dir=None, key=None):
        """
            check file exists
        Args:
            file_name:
            remote_dir:
            root_dir:
            key:

        Returns:

        """
        if not key:
            if not remote_dir or not root_dir:
                raise ValueError("args remote_dir or root_dir empty")
            file_name = os.path.basename(file_name)
            key = "{}/{}/{}".format(root_dir.strip("/"), remote_dir.strip("/"), file_name)
        return self.bucket.object_exists(key)

    def get_dir_info(self, prefix: str = "", cache: bool = False, **kwargs) -> dict:
        """
            获取指定前缀的目录内文件（递归）信息 例如：文件数、总文件大小
        Args:
            prefix:
            cache:
            **kwargs:

        Returns:
            {
                "count": 1,
                "size": 10000, # Byte
                "files": [
                        {
                            "name": "",
                            "size": "",
                        },
                    ], # 文件列表
            }

        """
        cache_filename = "cache_{}.json".format(hashlib.md5(prefix.encode()).hexdigest())
        cache_filename = os.path.join(temp_dir, cache_filename)
        if cache:
            # 尝试从本地获取
            if os.path.exists(cache_filename):
                with open(cache_filename, "r") as f:
                    info = json.load(f)
                return info

        file_count = 0
        file_size = 0
        files = []
        for obj in self.list(prefix=prefix):
            file_count += 1
            file_size += obj.size
            files.append({"name": obj.key, "size": obj.size})
        #
        files.sort(key=lambda x: x["name"])
        #
        info = {"count": file_count, "size": file_size, "files": files}
        with open(cache_filename, "w") as f:
            f.write(json.dumps(info))
        return info

    def list_dir(self, prefix: str = "", level: int = 1, **kwargs):
        """
            遍历指定目录下的子目录
        Args:
            prefix:
            level:
            **kwargs:

        Returns:

        """
        if level < 1:
            return
        prefix = "{}/".format(prefix.strip("/"))
        for obj in self.list(prefix=prefix, delimiter="/", **kwargs):
            if obj.is_prefix():
                if level == 1:
                    yield obj.key
                yield from self.list_dir(prefix=obj.key, level=level - 1)
        return

    def list(
        self, prefix: str = "", delimiter: str = "", marker="", **kwargs
    ) -> Generator[SimplifiedObjectInfo, None, None]:
        """
        遍历指定前缀的文件
            tips:
                1. oss 本无目录
                2. 若想递归遍历某个 prefix 下的全部文件 则仅设置prefix即可
        Args:
            prefix:
            delimiter:
                目录分隔符 默认空
                若设置此参数则
                    迭代过程中仅返回prefix下的文件和子文件夹名 而不对子文件夹进行递归
            marker:
            **kwargs:

        Returns:

        """
        obj_iter = oss2.ObjectIterator(
            self.bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=1000,
            max_retries=10,
            marker=marker,
        )
        for idx, obj in enumerate(obj_iter):
            yield obj
            if idx % 100 == 0:
                self.list_next_marker = obj_iter.next_marker

    def quit(self):
        return

    def upload(
        self,
        file_path,
        remote_dir="",
        root_dir="",
        remote_filename="",
        delete=False,
        overwrite=True,
        show_progress=False,
        **kwargs,
    ):
        """
        上传文件
            Examples:
                >>> OSSHandler().upload(__file__, "default", "/spider")
        Args:
            file_path: 本地文件路径
            remote_dir: 远程服务器文件夹
            root_dir: 远程服务器根文件夹
            delete: 上传完成是否删除本地文件 默认为 False
            overwrite: 是否覆盖现有文件 默认True
            show_progress:
            **kwargs:

        Returns:

        """
        if not remote_dir or not root_dir:
            raise ValueError("remote_dir or root_dir empty")
        file_name = os.path.basename(file_path)
        key = "{}/{}/{}".format(root_dir.strip("/"), remote_dir.strip("/"), remote_filename or file_name)
        if not overwrite:
            if self.bucket.object_exists(key):
                # 返回  meta
                self.logger.debug("文件已存在 不上传: {}".format(key))
                return self.bucket.get_object_meta(key)

        #
        def progress_callback(consumed_bytes, total_bytes):
            if total_bytes:
                rate = 100 * (float(consumed_bytes) / float(total_bytes))
                print("\r上传 {} -> {:.2f}%".format(file_name, rate), end="")
                sys.stdout.flush()

        resp = None
        for i in range(3):
            try:
                file_size = os.path.getsize(file_path)
                if file_size < 10 * 1024 * 1024:
                    resp = self.bucket.put_object_from_file(key, file_path)
                else:
                    # 分片大小
                    part_size = determine_part_size(file_size, preferred_size=1000 * 1024)
                    # 初始化分片
                    upload_id = self.bucket.init_multipart_upload(key).upload_id
                    parts = []
                    # 逐个上传分片
                    with open(file_path, "rb") as fileobj:
                        part_number = 1
                        offset = 0
                        while offset < file_size:
                            num_to_upload = min(part_size, file_size - offset)
                            result = self.bucket.upload_part(
                                key,
                                upload_id,
                                part_number,
                                SizedFileAdapter(fileobj, num_to_upload),
                            )
                            parts.append(PartInfo(part_number, result.etag))
                            offset += num_to_upload
                            part_number += 1
                            if show_progress:
                                progress_callback(part_size * part_number, file_size)
                    if show_progress:
                        # 这个是为了在连续上传时不把进度条覆盖掉
                        print("")
                    # 完成分片上传
                    resp = self.bucket.complete_multipart_upload(key, upload_id, parts)
                break
            except Exception as e:
                self.logger.exception(e)
                self.init()
        if delete:
            try:
                os.remove(file_path)
            except Exception as e:
                pass
        return resp

    def upload_object(self, file_obj, file_name, remote_dir="", root_dir="", overwrite=True, **kwargs):
        """
        上传文件对象 或者 文件内容
        Args:
            file_obj:
            file_name:
            remote_dir:
            root_dir:
            overwrite:
            **kwargs:

        Returns:

        """
        if not remote_dir or not root_dir:
            raise ValueError("args remote_dir or root_dir empty")
        key = "{}/{}/{}".format(root_dir.strip("/"), remote_dir.strip("/"), file_name)
        if not overwrite:
            if self.bucket.object_exists(key):
                # 返回  meta
                self.logger.debug("文件已存在 不上传: {}".format(key))
                return self.bucket.get_object_meta(key)
        resp = None
        for i in range(3):
            try:
                resp = self.bucket.put_object(key, file_obj)
                break
            except Exception as e:
                self.logger.exception(e)
                self.init()
        return resp

    def upload_object_part(self, data, file_name, upload_id, part_id, remote_dir="", root_dir=""):
        """
        上传分片内容

        Args:
            data:
            file_name:
            upload_id:
            part_id:
            remote_dir:
            root_dir:

        Returns:

        """
        key = "{}/{}/{}".format(root_dir.strip("/"), remote_dir.strip("/"), file_name)
        result = self.bucket.upload_part(key, upload_id, part_id, data)
        return result

    def complete_multipart_upload(self, file_name, upload_id, parts, remote_dir="", root_dir=""):
        """
        完成分片上传任务
        Args:
            file_name:
            upload_id:
            parts: 分片信息 [(part_id, etag), ...]
            remote_dir:
            root_dir:

        Returns:

        """
        key = "{}/{}/{}".format(root_dir.strip("/"), remote_dir.strip("/"), file_name)
        parts = [PartInfo(x[0], x[1]) for x in parts]
        return self.bucket.complete_multipart_upload(key, upload_id, parts)

    def restore_obj(self, key):
        r = self.bucket.restore_object(key)
        return r
