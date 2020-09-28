
# DRSObject

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **String** | An identifier unique to this &#x60;Object&#x60;. | 
**name** | **String** | A string that can be used to name an &#x60;Object&#x60;. This string is made up of uppercase and lowercase letters, decimal digits, hypen, period, and underscore [A-Za-z0-9.-_]. See http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_282[portable filenames]. |  [optional]
**selfUri** | **String** | A drs:// URI, as defined in the DRS documentation, that tells clients how to access this object. The intent of this field is to make DRS objects self-contained, and therefore easier for clients to store and pass around. | 
**size** | **Long** | For blobs, the blob size in bytes. For bundles, the cumulative size, in bytes, of items in the &#x60;contents&#x60; field. | 
**createdTime** | **String** | Timestamp of object creation in RFC3339. |  [optional]
**updatedTime** | **String** | Timestamp of &#x60;Object&#x60; update in RFC3339, identical to create timestamp in systems that do not support updates. |  [optional]
**version** | **String** | A string representing a version. (Some systems may use checksum, a RFC3339 timestamp, or an incrementing version number.) |  [optional]
**mimeType** | **String** | A string providing the mime-type of the &#x60;Object&#x60;. |  [optional]
**checksums** | [**List&lt;DRSChecksum&gt;**](DRSChecksum.md) | The checksum of the &#x60;Object&#x60;. At least one checksum must be provided. For blobs, the checksum is computed over the bytes in the blob.  For bundles, the checksum is computed over a sorted concatenation of the checksums of its top-level contained objects (not recursive, names not included). The list of checksums is sorted alphabetically (hex-code) before concatenation and a further checksum is performed on the concatenated checksum value.  For example, if a bundle contains blobs with the following checksums: md5(blob1) &#x3D; 72794b6d md5(blob2) &#x3D; 5e089d29  Then the checksum of the bundle is: md5( concat( sort( md5(blob1), md5(blob2) ) ) ) &#x3D; md5( concat( sort( 72794b6d, 5e089d29 ) ) ) &#x3D; md5( concat( 5e089d29, 72794b6d ) ) &#x3D; md5( 5e089d2972794b6d ) &#x3D; f7a29a04 | 
**accessMethods** | [**List&lt;DRSAccessMethod&gt;**](DRSAccessMethod.md) | The list of access methods that can be used to fetch the &#x60;Object&#x60;. Required for single blobs; optional for bundles. |  [optional]
**contents** | [**List&lt;DRSContentsObject&gt;**](DRSContentsObject.md) | If not set, this &#x60;Object&#x60; is a single blob. If set, this &#x60;Object&#x60; is a bundle containing the listed &#x60;ContentsObject&#x60; s (some of which may be further nested). |  [optional]
**description** | **String** | A human readable description of the &#x60;Object&#x60;. |  [optional]
**aliases** | **List&lt;String&gt;** | A list of strings that can be used to find other metadata about this &#x60;Object&#x60; from external metadata sources. These aliases can be used to represent secondary accession numbers or external GUIDs. |  [optional]



