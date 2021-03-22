#----SETUP-------
#pip3 install google-cloud-firestore==2.0.2
#unset GOOGLE_APPLICATION_CREDENTIALS
#gcloud auth application-default login
#GOOGLE_CLOUD_PROJECT=broad-jade-dev-data
#Examples: https://github.com/GoogleCloudPlatform/python-docs-samples/blob/6fff2b7cb43e062b4f584f135fee3b68b20a1e17/firestore/cloud-client/snippets.py#L929-L931
#Example Usage: python3 cleanup-snapshot-firestore.py

#-------------
#TODO: Right now, just lists files from firestore. Uncomment "TODO" lines to actually delete.
from google.cloud import firestore

#----USER DEFINE SOURCE DATASET AND SNAPSHOTID HERE----
sourceDatasetId='ecb5601e-9026-428c-b49d-3c5f1807ecb7'
snapshotId='4122d403-8d70-4922-ab6e-7db13e44a9a5'

#-----Step 3, part 3 - Dependencies-----
sourceDatasetFormatted = sourceDatasetId + '-dependencies'
db = firestore.Client()
query = db.collection(sourceDatasetFormatted).where(u'snapshotId', u'==', snapshotId)

def write_results_to_file(file_name):
    f = open(file_name, "a")
    f.write("==================File Dependency Firestore Delete===================\n")
    f.write(snapshotId)
    f.write("\n")

def delete_collection(coll_ref, batch_size):
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0

    for doc in docs:
        f.write(f'Deleting doc {doc.id} => {doc.to_dict()}\n')
        #TODO: When ready to delete - uncomment following line
        #doc.reference.delete()
        deleted = deleted + 1

    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size)


#TODO: When ready to delete, uncomment the following line
#write_results_to_file("dependencies_delete.txt")
#delete_collection(query, 10)
#f.close()

# Print number of files that would be deleted
results = query.stream()
print("dependencies: ")
print(sum(1 for k in results))



#-----Step 3, part 4 - files-----
file_query = db.collection(snapshotId)

#TODO: When ready to delete, uncomment the following line
#write_results_to_file("snapshot_file_delete.txt")
#delete_collection(file_query, 10)
#f.close()

file_results = file_query.stream()
print("files: ")
print(sum(1 for k in file_results))



