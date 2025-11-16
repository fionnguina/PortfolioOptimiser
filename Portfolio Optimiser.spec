# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_submodules
from PyInstaller.utils.hooks import collect_all

datas = [('Portfolio_Optimiser.py', '.')]
binaries = []
hiddenimports = ['beautifulsoup4', 'bs4', 'certifi', 'cffi', 'charset_normalizer', 'curl_cffi', 'datetime', 'dateutil', 'dateutil.tz', 'et_xmlfile', 'frozendict', 'hashlib', 'idna', 'io', 'json', 'multiprocessing', 'multitasking', 'numpy', 'openpyxl', 'pandas', 'pathlib', 'patsy', 'peewee', 'platformdirs', 'protobuf', 'pycparser', 'pythoncom', 'pytz', 'pywintypes', 're', 'requests', 'scipy', 'shutil', 'soupsieve', 'statsmodels', 'subprocess', 'sys', 'time', 'tkinter', 'tkinter.constants', 'tkinter.messagebox', 'tkinter.ttk', 'tzdata', 'urllib3', 'websockets', 'win32com', 'win32com.client', 'xlwings', 'yfinance']
hiddenimports += collect_submodules('beautifulsoup4')
hiddenimports += collect_submodules('bs4')
hiddenimports += collect_submodules('certifi')
hiddenimports += collect_submodules('cffi')
hiddenimports += collect_submodules('charset_normalizer')
hiddenimports += collect_submodules('curl_cffi')
hiddenimports += collect_submodules('datetime')
hiddenimports += collect_submodules('dateutil')
hiddenimports += collect_submodules('dateutil.tz')
hiddenimports += collect_submodules('et_xmlfile')
hiddenimports += collect_submodules('frozendict')
hiddenimports += collect_submodules('hashlib')
hiddenimports += collect_submodules('idna')
hiddenimports += collect_submodules('io')
hiddenimports += collect_submodules('json')
hiddenimports += collect_submodules('multiprocessing')
hiddenimports += collect_submodules('multitasking')
hiddenimports += collect_submodules('numpy')
hiddenimports += collect_submodules('openpyxl')
hiddenimports += collect_submodules('pandas')
hiddenimports += collect_submodules('pathlib')
hiddenimports += collect_submodules('patsy')
hiddenimports += collect_submodules('peewee')
hiddenimports += collect_submodules('platformdirs')
hiddenimports += collect_submodules('protobuf')
hiddenimports += collect_submodules('pycparser')
hiddenimports += collect_submodules('pythoncom')
hiddenimports += collect_submodules('pytz')
hiddenimports += collect_submodules('pywintypes')
hiddenimports += collect_submodules('re')
hiddenimports += collect_submodules('requests')
hiddenimports += collect_submodules('scipy')
hiddenimports += collect_submodules('shutil')
hiddenimports += collect_submodules('soupsieve')
hiddenimports += collect_submodules('statsmodels')
hiddenimports += collect_submodules('subprocess')
hiddenimports += collect_submodules('sys')
hiddenimports += collect_submodules('time')
hiddenimports += collect_submodules('tkinter')
hiddenimports += collect_submodules('tkinter.constants')
hiddenimports += collect_submodules('tkinter.messagebox')
hiddenimports += collect_submodules('tkinter.ttk')
hiddenimports += collect_submodules('tzdata')
hiddenimports += collect_submodules('urllib3')
hiddenimports += collect_submodules('websockets')
hiddenimports += collect_submodules('win32com')
hiddenimports += collect_submodules('win32com.client')
hiddenimports += collect_submodules('xlwings')
hiddenimports += collect_submodules('yfinance')
tmp_ret = collect_all('numpy')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('pandas')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('scipy')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('statsmodels')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('openpyxl')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('xlwings')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('yfinance')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]


a = Analysis(
    ['C:\\Users\\Fionn Guina\\Portfolio_Optimiser\\Main.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['xlwings.rest', 'scipy._lib.array_api_compat.torch'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='Portfolio Optimiser',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['C:\\Users\\Fionn Guina\\Portfolio_Optimiser\\icon.ico'],
)
