"""
Setup script for the data engineering pipeline package
"""

from setuptools import setup, find_packages

with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

with open('requirements.txt', 'r', encoding='utf-8') as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name='data-engineering-pipeline',
    version='1.0.0',
    author='Data Engineering Team',
    author_email='team@example.com',
    description='End-to-end data engineering pipeline with S3, Spark, and Airflow',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/lamle3105/engineering-proj',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    python_requires='>=3.8',
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'setup-pipeline=scripts.setup_pipeline:main',
            'generate-data=scripts.generate_sample_data:main',
        ],
    },
)
