from distutils.core import setup

setup(
    name='moex_connector',
    version='0.3.2',
    description='MOEX API connector',
    author='meznick',
    packages=['moex'],
    install_requires=[
        'requests>=2.28,<3',
        'pandas>2,<3',
    ]
)
