# Manual Snapshot Cleanup

## Step 3: Delete Snapshot Primary Data Step

1. **Manually Delete Snapshot Dataset from Big Query**
- [TODO] Manually delete in GCloud console

Remove references to Snapshot from source dataset:

2. **Remove ACLs from source dataset**
- [Done?]
- Everything looks okay here. All of the group policies on this dataset are correct, according to a SAM query. Am I missing something? I put together the "viewACLS.sh" to see all of the permissions in one spot
3. **Delete Snapshot File *Dependencies* from Firestore**
- [SKIP - not worth risk of deleting the wrong thing] Run cleanup-snapshot-firestore.py script
- Question: Getting 1785 entries to be deleted from firestore for one HCA snapshot. Does this number make sense?
4. **Delete Files for Snapshot from Firestore**
- [SKIP - not worth risk of deleting the wrong thing] Run cleanup-snapshot-firestore.py script
- Question: Getting 35195 entries to be deleted for this snapshot. Does this make sense?

## Step 4: Delete Snapshot Metadata Step
1. **Delete snapshot entry from postgres**
- [TODO] Run Query:
```
DELETE FROM snapshot WHERE id = :id
```
- Confirm rows affected is greater than 0
- Question: Do we not need to remove this snapshot from the other tables (e.g. snapshot_source)? Or would there be a cascade delete?

## Step 5: Unlock Snapshot
- [Unnecessary?]
- Question: Why would we need to do this on delete? Shouldn't the entry no longer exist after step 4?
```
UPDATE snapshot SET flightid = NULL " +
"WHERE id = :id AND flightid = :flightid"
```


