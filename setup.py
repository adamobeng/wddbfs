from setuptools import setup

setup(
    name="wddbfs",
    version="0.0.1",
    author="Adam Obeng",
    description="webdavfs provider which can read the contents of sqlite databases",
    entry_points={
        "console_scripts": ["wddbfs=wddbfs.cli:cli"],
    },
    packages=["wddbfs"],
    license_files=("LICENSE",),
    install_requires=[
        "cheroot",
        "wsgidav",
        "configargparse",
        "pandas",
    ],
)
