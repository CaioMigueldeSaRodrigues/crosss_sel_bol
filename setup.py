from setuptools import setup, find_packages

setup(
    name="scraping_benchmarking",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.21.0",
        "pyspark>=3.2.0",
        "openpyxl>=3.0.9",
        "requests>=2.26.0",
        "python-dotenv>=0.19.0",
        "scikit-learn>=0.24.2",
        "mlflow>=1.20.2",
        "matplotlib>=3.4.3",
        "seaborn>=0.11.2"
    ],
    author="Caio Miguel de Sá Rodrigues",
    author_email="caiomiguel@bemol.com.br",
    description="Sistema de recomendação de produtos para a Bemol",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/CaioMigueldeSaRodrigues/scraping_benchmarking",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
) 