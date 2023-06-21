from pprint import pprint
import docker
import subprocess as sp

class DockerException(Exception):
    pass

class API:
    def __init__(self):
        self.client = docker.from_env()
        self.container = None

    def containers(self):
        return [each.name for each in self.client.containers.list()]

    def pick_container(self, name=None):
        if not name:
            name = self.containers[0]
        self.container = self.client.containers.get(name)
        return self.container

    def attributes(self):
        if self.container:
            return dict(self.container.attrs)
        raise DockerException('No Container selected...')

    def logs(self, stream=False):
        return self.container.logs(stream=stream)

    def create(self, name):
        if name == 'SQL19':
            image = '''docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=D@t@t3@m!" -p 1433:1433 -d --mount type=bind,src=C:\\backup,target=/mnt/backup --name SQL19 mcr.microsoft.com/mssql/server:2019-latest  '''
            sp.Popen(image, universal_newlines=True, shell=True, stdout=sp.PIPE, stderr=sp.PIPE).communicate()
            bkf = '''docker exec -it SQL19 mkdir /var/opt/mssql/backup '''
            sp.Popen(bkf, universal_newlines=True, shell=True, stdout=sp.PIPE, stderr=sp.PIPE).communicate()
            return self.pick_container('SQL19')
        elif name == 'firebird':
            image = '''docker run -p 127.0.0.1:3050:3050 -d --name Firebird3 firebird '''
            sp.Popen(image, universal_newlines=True, shell=True, stdout=sp.PIPE, stderr=sp.PIPE).communicate()
            return self.pick_container('Firebird3')


if __name__=='__main__':
    d = API()
    d.pick_container('SQL19')
    print(d.attributes())
    x = d.logs()
    print(x.decode())

