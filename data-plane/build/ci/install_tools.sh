#!/usr/bin/env bash

sudo apt-get install -y gettext-base
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.8.1/kind-"$(uname)"-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind
kind version
curl -LO https://storage.googleapis.com/kubernetes-release/release/"$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)"/bin/linux/amd64/kubectl
chmod +x ./kubectl
sudo mv ./kubectl /usr/local/bin/kubectl
kubectl version --client
