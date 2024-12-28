from setuptools import setup, find_packages
setup(
name='snow2dbt',
version='0.2.1',
author='Tensor',
author_email='clementparsy02@gmail.com',
description='Command Line to reverse engineering a Snowflake Table to DBT Model file',
packages=find_packages(),
classifiers=[
'Programming Language :: Python :: 3',
'License :: OSI Approved :: MIT License',
'Operating System :: OS Independent',
],
python_requires='>=3.7',
entry_points = {
    'console_scripts': [
        'snow2dbt=snow2dbt.snow2dbt:snow2dbt'
    ]
}
)