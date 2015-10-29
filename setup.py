from distutils.core import setup

setup(
    name='basilisk',
    packages=['basilisk'],
    version='0.1',
    install_requires=[
        'six',
        'redis',
        'elasticsearch'
    ],
    description='Basilisk is a object-NoSQL mapper for Python 2.7 and 3.3+,'
                'supporting models, lists, hashes and sorted sets.',
    author='Bonnier Business Polska / Krzysztof Bujniewicz',
    author_email='racech@gmail.com',
    url='https://github.com/bonnierpolska/basilisk',
    download_url='https://github.com/bonnierpolska/basilisk/tarball/0.1',
    keywords=['orm', 'nosql', 'mapper', 'redis', 'elasticsearch'],
    classifiers=[]
)
