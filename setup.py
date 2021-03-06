from setuptools import setup, find_packages

requires = [ "pyCLI>=2.0.3", "boto>=2.14.0", "futures>=2.1.4", "raven==4.2.3" ]

setup(name="zsync",
      version="0.1",
      platforms='any',
      packages = find_packages(),
      include_package_data=True,
      install_requires=requires,
      author = "PressLabs SRL",
      author_email = "contact@presslabs.com",
      url = "https://github.com/presslabs/zsync",
      description = "ZFS snapshot synchronization for ninjas",
      entry_points = {'console_scripts': [ 'zsync = zsync.runner:execute_from_cli' ]},
      test_requirements = ['nose>=1.2.1','coverage>=3.5.2','mock>=1.0.1', "nosexcover>=1.0.8"],
      classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Financial and Insurance Industry',
        'Intended Audience :: Science/Research',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'License :: OSI Approved :: BSD License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Topic :: System :: Networking',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.0',
        'Programming Language :: Python :: 3.1',
        'Programming Language :: Python :: 3.2',
      ]
)
