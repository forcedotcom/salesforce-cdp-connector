from setuptools import setup, find_packages

setup(
    name='salesforce-cdp-connector',
    version='1.0.0',
    packages=find_packages(include=['salesforcecdpconnector', 'salesforcecdpconnector.*']),
    url='',
    license='BSD-3-Clause',
    author='vishnu.prasad',
    author_email='',
    description='Python Connector for Salesforce CDP',
    install_requires=[
        'certifi==2021.10.8',
        'charset-normalizer==2.0.10',
        'idna==3.3',
        'numpy==1.22.2',
        'pandas==1.3.5',
        'pip==22.0.4',
        'pyarrow==4.0.0',
        'python-dateutil==2.8.2',
        'pytz==2021.3',
        'requests==2.27.1',
        'responses==0.16.0',
        'setuptools==60.9.3',
        'six==1.16.0',
        'urllib3==1.26.8',
        'wheel==0.37.1'
    ],
    python_requires='>=3'
)
