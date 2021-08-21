from setuptools import find_packages, setup
setup(
    name='sparkpip',
    packages=find_packages(include=['sparkpip']),
    version='0.1.0',
    description='Base classes for working with pyspark in pipeline concept',
    author='n-surkov',
    license='',
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==4.4.1'],
    test_suite='tests'
)
