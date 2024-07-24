# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""A module containing 'FileStorage' and 'FilePipelineStorage' models."""

import logging
import os
import re
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import aiofiles
from aiofiles.os import remove
from aiofiles.ospath import exists
from datashaper import Progress

from graphrag.index.progress import ProgressReporter

from .typing import PipelineStorage

import requests

import platform
from smb.SMBConnection import SMBConnection
import docx, openpyxl, pptx, os.path
import io

log = logging.getLogger(__name__)


class FilePipelineStorage(PipelineStorage):
    """File storage class definition."""

    _root_dir: str
    _encoding: str

    def __init__(self, root_dir: str | None = None, encoding: str | None = None):
        """Init method definition."""
        self._root_dir = root_dir or ""
        self._encoding = encoding or "utf-8"
        Path(self._root_dir).mkdir(parents=True, exist_ok=True)

    ####text html md csvファイル（拡張子：.txt .html .md .csv)からテキスト抽出するメソッド
    def get_text_from_binary(file: io.BytesIO) -> str:
        # ファイルオブジェクトの内容をバイナリモードで読み込む
#        raw_data = file.read()
        # chardetを使用して文字コードを検出
#        encoding = chardet.detect(raw_data)['encoding']
        # ファイルオブジェクトのポインタを先頭に戻す
        file.seek(0)
        # 検出した文字コードでファイルオブジェクトを読み込み、全内容を一つの文字列として取得する
#        content = file.read().decode(encoding)
        
        content = file.read().decode()
        return content
    
    ####PDFファイル（拡張子：.pdf)からテキスト抽出するメソッド
    def get_text_from_pdf(file: io.BytesIO) -> str:
        output = io.StringIO()
        extract_text_to_fp(file, output)
        return output.getvalue()
    
    ####PowerPointファイル（拡張子：.pptx)からテキスト抽出するメソッド
    def get_text_from_powerpoint(file: io.BytesIO) -> str:
        prs = pptx.Presentation(file)
        output = ""
        for i, sld in enumerate(prs.slides, start=1):
            for shp in sld.shapes:
                if shp.has_text_frame:
                    text = shp.text
                    output += text.replace("\n", "").replace("\n", "").replace(" ", "").replace("　", "")
        return output
    
    
    ####Wordファイル（拡張子：docx)からテキスト抽出するメソッド
    def get_text_from_word(file: io.BytesIO) -> str:
        document = docx.Document(file)
        text_list = list(map(lambda par: par.text, document.paragraphs))
        text = "".join(text_list)
        output = text.replace(" ", "").replace("　", "")
        return output
    
    ####Excelファイル（拡張子：.xlsx)からテキスト抽出するメソッド
    def get_text_from_excel(file: io.BytesIO) -> str:
        output_text = ""
        book = openpyxl.load_workbook(file)
        sheet_list = book.sheetnames
        for sheet_name in sheet_list:
            sheet = book[sheet_name]
            output_text += f"{sheet_name}\n"
            for cells in tuple(sheet.rows):
                for cell in cells:
                    data = cell.value
                    if data is None:
                        continue
                    else:
                        #output_text += str(data).replace(" ", "").replace("　", "")
                        output_text += str(data)
                output_text += "\n"
            output_text += "\n\n"
        return output_text

    ####Key: ファイルの拡張子に応じて、Value: 適切なメソッドを選択する際に用いる辞書
    func_dict = {
        'pdf' : get_text_from_pdf,
        'docx'  : get_text_from_word,
        'xlsx'  : get_text_from_excel,
        'xlsm'  : get_text_from_excel,
        'pptx' : get_text_from_powerpoint,
        'txt' : get_text_from_binary,
        'html' : get_text_from_binary,
        'md' : get_text_from_binary,
        'csv' : get_text_from_binary
             }

    def find(
        self,
        file_pattern: re.Pattern[str],
        base_dir: str | None = None,
        progress: ProgressReporter | None = None,
        file_filter: dict[str, Any] | None = None,
        max_count=-1,
        userid: str  | None = None,
        password: str  | None = None,
        share_directory: str  | None = None,
        file_server: str  | None = None,
        file_path: str  | None = None,
    ) -> Iterator[tuple[str, dict[str, Any]]]:
        """Find files in the storage using a file pattern, as well as a custom filter function."""
        search_path = Path(self._root_dir) / (base_dir or "")
        #all_files = list(search_path.rglob("**/*"))
        #all_files = requests.post("http://10.2.230.41:8000/filelist4graph", json = {"directoryname": "393"}).json()
        SMBUSER = userid
        SMBPASS = password
        RMHOST = "10.2.230.40"
        RMADDR = file_server
        RMPORT = 445
        
        #SMBConnection
        conn = SMBConnection(
            SMBUSER,
            SMBPASS,
            platform.uname().node,
            RMHOST,
            use_ntlm_v2=True,
            is_direct_tcp=True)
        
        conn.connect(RMADDR, RMPORT)
        # 検索するファイルパターン
        patterns = ['*.docx', '*.pdf', '*.xlsx', '*.xlsm', '*.pptx', '*.txt', '*.html', '*.md', '*.csv']
        items = []
        
        try:
            for pattern in patterns:
                files = conn.listPath(share_directory, file_path, pattern=pattern)
                items.extend(files)
        except Exception as e:
            print(f"Error: {e}")
        all_files = [item.filename for item in items]
        conn.close()
        num_loaded = 0
        num_total = len(all_files)
        num_filtered = 0
        for file in all_files:
            yield (file, {})
            num_loaded += 1
            if max_count > 0 and num_loaded >= max_count:
                break
            if progress is not None:
                progress(_create_progress_status(num_loaded, num_filtered, num_total))

    async def get(
        self, key: str, as_bytes: bool | None = False, encoding: str | None = None
    ) -> Any:
        """Get method definition."""
        file_path = join_path(self._root_dir, key)

        if await self.has(key):
            return await self._read_file(file_path, as_bytes, encoding)
        if await exists(key):
            # Lookup for key, as it is pressumably a new file loaded from inputs
            # and not yet written to storage
            return await self._read_file(key, as_bytes, encoding)

        return None

    async def getText(
        self, key: str, as_bytes: bool | None = False, encoding: str | None = None, userid: str | None = None, password: str | None = None, share_directory: str | None = None, file_server: str | None = None, file_path: str | None = None
    ) -> Any:
        """Get method definition."""
        #file_path = join_path(self._root_dir, key)
        response = ""
        SMBUSER = userid
        SMBPASS = password
        RMHOST = "10.2.230.40"
        RMADDR = file_server
        RMPORT = 445
        
        #SMBConnection
        conn = SMBConnection(
            SMBUSER,
            SMBPASS,
            platform.uname().node,
            RMHOST,
            use_ntlm_v2=True,
            is_direct_tcp=True)
        
        conn.connect(RMADDR, RMPORT)
#        items = conn.listPath(share_directory, file_path, pattern = '*.docx|*.pdf|*.txt')
        _, ext = os.path.splitext(key)
        ext = ext[1:].lower()
        with io.BytesIO() as file:
            conn.retrieveFile('anthra', f'{file_path}/{key}', file)
            file.seek(0)
            if ext in func_dict:
                func = func_dict[ext]
                response = func(file)
        conn.close()
        #response = requests.post("http://10.2.230.41:8000/filestring4graph", json = {"directoryname": "393", "filename" : key}).json()
        return response

    async def _read_file(
        self,
        path: str | Path,
        as_bytes: bool | None = False,
        encoding: str | None = None,
    ) -> Any:
        """Read the contents of a file."""
        read_type = "rb" if as_bytes else "r"
        encoding = None if as_bytes else (encoding or self._encoding)

        async with aiofiles.open(
            path,
            cast(Any, read_type),
            encoding=encoding,
        ) as f:
            return await f.read()

    async def set(self, key: str, value: Any, encoding: str | None = None) -> None:
        """Set method definition."""
        is_bytes = isinstance(value, bytes)
        write_type = "wb" if is_bytes else "w"
        encoding = None if is_bytes else encoding or self._encoding
        async with aiofiles.open(
            join_path(self._root_dir, key), cast(Any, write_type), encoding=encoding
        ) as f:
            await f.write(value)

    async def has(self, key: str) -> bool:
        """Has method definition."""
        return await exists(join_path(self._root_dir, key))

    async def delete(self, key: str) -> None:
        """Delete method definition."""
        if await self.has(key):
            await remove(join_path(self._root_dir, key))

    async def clear(self) -> None:
        """Clear method definition."""
        for file in Path(self._root_dir).glob("*"):
            if file.is_dir():
                shutil.rmtree(file)
            else:
                file.unlink()

    def child(self, name: str | None) -> "PipelineStorage":
        """Create a child storage instance."""
        if name is None:
            return self
        return FilePipelineStorage(str(Path(self._root_dir) / Path(name)))


def join_path(file_path: str, file_name: str) -> Path:
    """Join a path and a file. Independent of the OS."""
    return Path(file_path) / Path(file_name).parent / Path(file_name).name


def create_file_storage(out_dir: str | None) -> PipelineStorage:
    """Create a file based storage."""
    log.info("Creating file storage at %s", out_dir)
    return FilePipelineStorage(out_dir)


def _create_progress_status(
    num_loaded: int, num_filtered: int, num_total: int
) -> Progress:
    return Progress(
        total_items=num_total,
        completed_items=num_loaded + num_filtered,
        description=f"{num_loaded} files loaded ({num_filtered} filtered)",
    )
