namespace=$1
if [ -z "$namespace" ]
then
  echo "namespace cannot be empty"
  exit 1
fi

kubectl get secret -n ${namespace} --no-headers=true | awk '/sh-inuse/{print $1}' | xargs kubectl delete secret -n ${namespace}

