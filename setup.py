from __future__ import absolute_import

from setuptools import find_packages, setup

with open('VERSION') as version_fd:
    version = version_fd.read().strip()

with open('README.rst', 'r') as readme_fd:
    long_description = readme_fd.read()

setup(
    name='kinesis-python-enhanced',
    version=version,
    description='Low level, multiprocessing based AWS Kinesis producer & consumer library, with additional features',
    long_description=long_description,
    url='https://github.com/nicholasball/kinesis-python-enhanced',

    install_requires=[
        'boto3>=1.4.4,<2.0',
        'offspring>=0.0.3,<1.0',
        'six>=1.11.0,<2.0',
    ],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    author='Nicholas Ball',
    author_email='nicholas@incorpleo.com',
    license='Apache License Version 2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries'
    ]
)
