import os
import shutil
import subprocess

from setuptools import setup, find_packages
import setuptools.command.build_ext

class build_ext(setuptools.command.build_ext.build_ext):
    def run(self):
        subprocess.check_call(["bazel", "build", "//python/ray:_ray.so"])
        source = os.path.join("..", "bazel-bin", "python", "ray", "_ray.so")
        target = os.path.join("ray", "_ray.so")
        shutil.copy(source, target)

class BinaryDistribution(setuptools.Distribution):
    def has_ext_modules(self):
        return True

setup(
    name="ray",
    version="0.0.1",
    packages=find_packages(),
    cmdclass={"build_ext": build_ext},
    distclass=BinaryDistribution,
    include_package_data=True,
    zip_safe=False,
    license="Apache 2.0"
)