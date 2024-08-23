# setup.py
from setuptools import find_packages, setup

setup(
    name="immaterialdb",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        # List your dependencies here, e.g.,
        # 'requests',
    ],
    author="James Brock",
    author_email="contact@dysomni.com",
    description="A materialized graph based dynamodb client",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/my_package",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)
