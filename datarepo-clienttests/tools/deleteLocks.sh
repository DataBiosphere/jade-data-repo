namespace=$1
kubectl get secret -n ${namespace} --no-headers=true | awk '/sh-inuse/{print $1}' | xargs kubectl delete secret -n ${namespace}

