#pip3 install google-cloud-firestore==2.0.2
#unset GOOGLE_APPLICATION_CREDENTIALS
#gcloud auth application-default login
#GOOGLE_CLOUD_PROJECT=broad-jade-dev-data
#Examples: https://github.com/GoogleCloudPlatform/python-docs-samples/blob/6fff2b7cb43e062b4f584f135fee3b68b20a1e17/firestore/cloud-client/snippets.py#L929-L931

from google.cloud import firestore

sourceDatasetId='ecb5601e-9026-428c-b49d-3c5f1807ecb7'
snapshotId='4122d403-8d70-4922-ab6e-7db13e44a9a5'

sourceDatasetFormatted = sourceDatasetId + '-dependencies'
db = firestore.Client()
query = db.collection(sourceDatasetFormatted).where(u'snapshotId', u'==', snapshotId)
results = query.stream()
print(sum(1 for k in results))
#result: 17585
#for k in results:
#    print(k.id)
#TODO: add delete command