from setuptools import setup, find_packages

__version__ = '0.1'

requirements = [
    'flask',
    'flask-sqlalchemy',
    'flask-restful',
    'flask-migrate',
    'flask-jwt-extended',
    'flask-marshmallow',
    'marshmallow-sqlalchemy',
    'python-dotenv',
    'passlib',
    'celery',
    'pandas',
    'datatable',
    'tqdm',
    'pyspark',
    'rpy2',
    'numpy',
    'sklearn',
    'tzlocal',
    'seaborn',
    'matplotlib'
]
setup(
    name='UX',
    version=__version__,
    packages=find_packages(exclude=['tests']),
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'UX = UX.manage:cli'
        ]
    }
)
