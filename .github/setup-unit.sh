curl -Lo ./kind "https://kind.sigs.k8s.io/dl/v0.31.0/kind-linux-amd64"
chmod +x ./kind
./kind create cluster
curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x ./kubectl

docker pull ubuntu
./kind load docker-image ubuntu

docker pull busybox
./kind load docker-image busybox


docker pull debian:stable-slim
./kind load docker-image debian:stable-slim