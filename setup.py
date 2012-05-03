#!/usr/bin/python

from setuptools import setup

setup(
    name="taskmaster",
    license='Apache License 2.0',
    version="0.1.0",
    description="",
    author="David Cramer",
    author_email="dcramer@gmail.com",
    url="https://github.com/dcramer/taskmaster",
    packages=["taskmaster", "taskmaster.master", "taskmaster.slave"],
    package_dir={'': 'src'},
    entry_points={
        'console_scripts': [
            'tm-master = taskmaster.master:main',
            'tm-slave = taskmaster.slave:main',
        ],
    },
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Topic :: Software Development",
        "Topic :: Utilities",
    ])
