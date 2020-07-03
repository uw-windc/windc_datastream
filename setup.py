import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="windc_data",
    version="0.1.6",
    author="Adam Christensen",
    author_email="achristensen@gams.com",
    description="A helper package to read/write WiNDC Data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://windc.wisc.edu",
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    install_requires=[
        "gams",
        "ipython",
        "matplotlib",
        "networkx",
        "pandas",
        "pip",
        "tqdm",
        "xlrd",
        "pip",
        "argparse",
        "schema",
        "terminaltables",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
