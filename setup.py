from setuptools import setup, find_packages

with open('VERSION') as version_fd:
    version = version_fd.read().strip()

install_requires = [
    'boto3>=1.4.4,<2.0'
]

setup(
    name='kinesis-python',
    version=version,
    install_requires=install_requires,
    packages=find_packages('src'),
    package_dir={'': 'src'},
    author='Evan Borgstrom',
    author_email='eborgstrom@nerdwallet.com',
    license='Apache 2',
    description='Low level, multiprocessing based AWS Kinesis producer & consumer library'
)
