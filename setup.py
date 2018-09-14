
from distutils.core import setup

setup(
	name='TransPred',
	version='0.1.0',
	author='siri surab',
	author_email='siri.surab@gmail.com',
	packages=['src'],
	scripts=['bin/get_cab_data.sh','bin/get_traffic_n_process.sh','bin/get_transit_data.sh','bin/process_traffic_data.py'],	
	install_requires=[
	],
)
