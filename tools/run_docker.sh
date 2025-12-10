# This access token is for registry (docker) read only in our organization. There is no risk associated with this
# except someone else being able to download the docker.

docker login registry.gitlab.com -u xmival00 -p glpat-n0UORgsMKJ5oKL7lt-o9OW86MQp1OjIxd3h3Cw.01.121uf4gm6
docker pull registry.gitlab.com/bnel-neuro/bnel-core/bnel-mef3-server
docker run -d -p 50051:50051 --name bnel-mef3-server registry.gitlab.com/bnel-neuro/bnel-core/bnel-mef3-server:latest
