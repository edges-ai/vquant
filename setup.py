from setuptools import setup, find_packages

# Long description from README (assuming it exists)
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    # Project metadata
    name="vquant",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A quantitative research library for factor and signal analysis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/vquant",  # Replace with your repo URL
    license="MIT",
    keywords=["quantitative", "finance", "factors", "signals", "research"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Mathematics",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
    ],
    
    # Package details
    packages=find_packages(include=["vquant", "vquant.*"]),
    python_requires=">=3.9",
    
    # Dependencies
    install_requires=[
        "polars>=0.20.0",
        "pyarrow>=12.0.0",
        "numpy>=1.23.0",
        "matplotlib>=3.7.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "black>=23.7.0",
            "flake8>=6.1.0",
            "mypy>=1.5.0",
        ],
        "docs": [
            "sphinx>=7.0.0",
            "sphinx-rtd-theme>=1.3.0",
        ],
    },
    
    # Entry points (optional, e.g., for CLI tools)
    # entry_points={
    #     "console_scripts": [
    #         "vquant = vquant.cli:main",  # If you add a CLI later
    #     ],
    # },
)