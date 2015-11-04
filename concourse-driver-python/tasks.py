__author__ = 'jnelson'

from invoke import task, run


@task
def build():
    run('python setup.py clean sdist bdist_wheel')
    #run('twine upload dist/*')

@task
def clean():
    run('rm -rf build')

@task
def docs():
    run('pdoc concourse/ --html --overwrite --html-dir build/docs')


@task
def test():
    run('nosetests')

