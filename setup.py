from setuptools import setup, find_packages

with open('VERSION') as version_fd:
    version = version_fd.read().strip()

#with open('README.rst', 'r') as readme_fd:
#    long_description = readme_fd.read()
long_description = 'Low level, multiprocessing based AWS Kinesis producer & consumer library'

setup(
    name='kinesis-python',
    version=version,
    description='Low level, multiprocessing based AWS Kinesis producer & consumer library',
    long_description=long_description,
    url='https://github.com/NerdWalletOSS/kinesis-python',

    install_requires=[
        'boto3>=1.4.4,<2.0',
        'six>=1.11.0,<2.0',
    ],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    author='Evan Borgstrom',
    author_email='eborgstrom@nerdwallet.com',
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
