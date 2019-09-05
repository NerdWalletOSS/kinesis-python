#!groovy

parallel(
    python3: {
        indy {
            base = 'ubuntu-2018.12.18'
            env = [
                // We skip auto-versioning on Python 3, it will happen on the Python 2 block below
                'DISABLE_AUTO_VERSION=1',
                'PYTHON=python3.7',
            ]
        }
    },
    python2: {
        indy {
            base = 'ubuntu-2018.12.18'
            env = [
                'PYTHON=python2',
            ]
        }
    },
    failFast: true
)